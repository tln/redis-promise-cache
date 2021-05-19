const Redis = require("ioredis");
const util = require('util')
const debug = require('debug')('redis-promise-cache')

async function connect(redisOptions) {
    const connection = new Connection(redisOptions)
    return connection.connect()
}
exports.connect = connect

class Connection {
    constructor(redisOptions={}) {
        redisOptions = {...redisOptions, lazyConnect: true};
        this.redis = new Redis(redisOptions)
        this.sub = new Redis(redisOptions)
        this.client = 'client_'+Date.now()+'_'+process.pid
        this.callbacks = {}
        this.pings = {}
        this.promises = {length: 0}
    }
    async connect() {
        await Promise.all([this.redis.connect(), this.sub.connect()])
        const clientHandler = shift(dispatch({
            ping: peer => this.redis.publish(peer, 'pong '+this.client),
            pong: dispatch(this.pings, {once: true}),
        }))
        this.sub.on('message', listHandler([
            logHandler('message'),
            channelsHandler({[this.client]: clientHandler}),
            dispatch(this.callbacks, {once: true, unsub: true}),
            logHandler('unhandled')
        ]))
        await this.sub.subscribe(this.client)
        return this
    }
    Map(...args) {
        return new RedisMap(this, ...args)
    }
    close() {
        // TODO can we detect if there are things waiting on this, 
        // and maybe cancel them?
        this.redis.disconnect()
        this.sub.disconnect()    
    }
    ifDead(peer, value, timeout=200) {
        // resolve to value if peer is dead,
        // otherwise never resolves
        return new Promise((resolve) => {
            const timer = setTimeout(() => {
                debug(peer, 'is dead');
                resolve(value);
            }, timeout);
            addCallback(this.pings, peer, () => clearTimeout(timer));
            this.redis.publish(peer, 'ping '+this.client)
        })

    }
    ping(peer, timeout=2000) {
        // ping peer, resolve to elapsed time or reject if peer is dead
        const start = Date.now()
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject('no pong from '+peer), timeout);
            addCallback(this.pings, peer, () => {
                clearTimeout(timer);
                resolve(Date.now() - start)
            })
            this.redis.publish(peer, 'ping '+this.client)
        })
    }
    subOnce(channel, callback) {
        addCallback(this.callbacks, channel, callback)
        this.sub.subscribe(channel)
    }
    storePromise(promise, callback) {
        const id = this.promises.length++;
        this.promises[id] = promise
        const token = `promise-${this.client}-${id}`
        this.resolvePromise(token, promise, callback)
        return token
    }
    async resolvePromise(token, promise, callback) {
        let result, redisString
        try{ 
            result = await promise;
            redisString = JSON.stringify(result)
        } catch(error) {
            result = new Tombstone(error)
            redisString = result.toRedis()
        }
        callback(result) // ignores error!
        debug('publish', token, redisString)
        this.redis.publish(token, redisString)
    }
    waitForPromise(token, fallback) {
        let [_, promiseClient, id] = token.split('-')
        if (promiseClient == this.client) {
            // TODO Can we delete it now?
            return this.promises[id];
        } else {
            debug('waiting for', promiseClient, id)
            const promise = new Promise(resolve => 
                this.subOnce(token, message => resolve(fromRedis(message)))
            )
            return Promise.race([promise, this.ifDead(promiseClient).then(fallback)])
        }
    }
    toRedis(value, writeback) {
        if (util.types.isPromise(value)) {
            debug('storing a promise')
            return this.storePromise(value, writeback)
        } else if (value instanceof Tombstone) {
            return value.toRedis()
        }
        return JSON.stringify(value)
    }
    fromRedis(data, fallback) {
        if (data === null) return null
        if (Tombstone.isTombstone(data)) return Tombstone.dataToTombstone(data)
        if (isPromiseToken(data)) return this.waitForPromise(data, fallback)
        try {
            return JSON.parse(data)
        } catch(e) {
            console.error('Error parsing JSON', e, data)
            throw e;
        }
    }
}

// -- Channel handling
function drop(channel, message) { return true; }
function logHandler(string) {
    return (channel, message) => debug(string, channel, message)
}
function listHandler(handlers) {
    return (channel, message) => {
        for (const handler of handlers) {
            if (handler(channel, message)) return true
        }
        return false
    }
}
function channelsHandler(channels, unhandled=drop) {
    return (channel, message) => {
        const handler = channels[channel] || unhandled;
        return handler(channel, message)
    }
}
function dispatch(callbacks, {once=false, unsub=false}={}) {
    return (channel, message) => {
        const cb = callbacks[channel]
        if (!cb) return false;
        if (typeof cb === 'function') cb(message)
        else cb.forEach(fn => fn(message))
        if (once) delete callbacks[channel]
        if (unsub) sub.unsubscribe(channel)
        return true;
    }
}
function shift(handler) {
    return (ch, msg) => ([ch, ...msg] = msg.split(' '), handler(ch, msg.join(' ')))
}
function addCallback(callbacks, channel, fn) {
    if (!callbacks[channel]) callbacks[channel] = [fn]
    else callbacks[channel].push(fn)
}

// --- Promise tokens
function isPromiseToken(s) {
    return s.startsWith('promise-')
}

// --- value mapping
class Tombstone {
    constructor(error) {
        this.error = error
    }
    static isTombstone(value) {
        if (typeof value === 'string') return value.startsWith('tombstone ')
        else return value instanceof Tombstone
    }
    static dataToTombstone(string) {
        console.assert(Tombstone.isTombstone(string))
        return Promise.reject(JSON.parse(string.slice(10)))
    }
    toPromise() {
        return Promise.reject(this.error)
    }
    toRedis() {
        return 'tombstone '+JSON.stringify(this.error||'unknown error')
    }
}

// -- redis map
class RedisMap {
    constructor(connection, hkey, {writeback='noErrors'}={}) {
        this.conn = connection
        this.hkey = hkey
        this.writebackPolicy = writebackPolicy(writeback)
    }
    async get(key) {
        const data = await this.conn.redis.hget(this.hkey, key)
        return this.conn.fromRedis(data, () => this.fallback(key))
    }
    set(key, value) {
        const data = this.conn.toRedis(value, value => this.writeback(key, value))
        return this.conn.redis.hset(this.hkey, key, data)
    }
    fallback(key) {
        // getting the key failed (was a promise, peer died)
        // write back (without waiting) a null so we don't repeat this process over nd over
        this.writeback(key, null)
        return null
    }
    writeback(key, value) {
        // different behavior on errors
        if (this.writebackPolicy(value, key)) {
            debug('writing back', key, value)
            return this.conn.redis.hset(this.hkey, key, this.conn.toRedis(value))
        } else {
            debug('not writing back', key, value)
            // Delete the promise
            return this.conn.redis.hdel(this.hkey, key)
        }
    }
    delete(key) {
        // TODO handle with promises
        return redis.hdel(this.hkey, key)
    }
    dump() {
        console.log('Dump not implemented')
    }
}
function writebackPolicy(policy) {
    if (policy === 'noErrors') return noErrors
    if (policy == 'all') return anyValue
    if (Function.isFunction(policy)) return functionPolicy(policy)
}
function noErrors(value) {
    return !(value instanceof Tombstone)
}
function anyValue(value) {
    return true
}
function functionPolicy(errorFunction) {
    return value => !(value instanceof Tombstone) || errorFunction(value.error)
}
exports.RedisMap = RedisMap