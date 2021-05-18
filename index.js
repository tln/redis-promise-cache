const Redis = require("ioredis");
const util = require('util')
const debug = require('debug')('redis-promise-cache')

async function connect(redisOptions) {
    const connection = new Connection(redisOptions)
    return connection.connect()
}
exports.connect = connect

class Connection {
    constructor(redisOptions) {
        redisOptions.lazyConnect = true;
        this.redis = new Redis(redisOptions)
        this.sub = new Redis(redisOptions)
        this.client = 'client_'+Date.now()+'_'+process.pid
        this.callbacks = {}
        this.pings = {}
        this.promises = {}
    }
    async connect() {
        await Promise.all([this.redis.connect(), this.sub.connect()])
        const clientHandler = shift(dispatch({
            ping: peer => this.redis.publish(peer, 'pong '+client),
            pong: dispatch(this.pings, {once: true}),
        }))
        sub.on('message', listHandler([
            logHandler('message'),
            channelsHandler({[this.client]: clientHandler}),
            dispatch(this.callbacks, {once: true, unsub: true}),
            logHandler('unhandled')
        ]))
        await this.sub.subscribe(client)
    }
    Map(...args) {
        return new RedisMap(this, ...args)
    }
    close() {
        redis.disconnect()
        sub.disconnect()    
    }
    ping(peer, timeout=2000) {
        const start = Date.now()
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject('no pong from '+peer), timeout);
            addCallback(this.pings, peer, () => (clearTimeout(timer), resolve(Date.now() - start)))
            this.redis.publish(peer, 'ping '+client)
        })
    }
    subOnce(channel, callback) {
        addCallback(this.callbacks, channel, callback)
        this.sub.subscribe(channel)
    }
    storePromise(value, callback) {
        const id = this.promises.length++;
        this.promises[id] = value
        const token = `promise-${client}-${id}`
        this.resolvePromise(token, value, callback)
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
    waitForPromise(token) {
        let [_, promiseClient, id] = token.split('-')
        if (promiseClient == this.client) {
            // TODO Can we delete it now?
            return this.promises[id];
        } else {
            debug('waiting for', promiseClient, id)
            this.ping(promiseClient)
            return new Promise(resolve => 
                this.subOnce(token, message => resolve(fromRedis(message)))
            )
        }
    }
    toRedis(value, callback) {
        if (util.types.isPromise(value)) {
            debug('storing a promise')
            return this.storePromise(value, callback)
        } else if (value instanceof Tombstone) {
            return value.toRedis()
        }
        return JSON.stringify(value)
    }
    fromRedis(data) {
        if (data === null) return null
        if (Tombstone.isTombstone(data)) return Tombstone.dataToTombstone(data)
        if (isPromiseToken(data)) return this.waitForPromise(data)
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
function dispatch(callbacks, {once=false, unsub=false}) {
    return (channel, message) => {
        if (!callbacks[channel]) return false;
        callbacks[channel].forEach(fn => fn(message));
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
    static isTombstone(string) { 
        return string.startsWith('tombstone ')
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
    get(key) {
        return this.conn.redis.hget(this.hkey, key).then(data => this.conn.fromRedis(data))
    }
    set(key, value) {
        const data = this.conn.toRedis(value, value => this.writeback(key, value))
        return this.conn.redis.hset(this.hkey, key, data)
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
    return !Tombstone.isTombstone(value)
}
function anyValue(value) {
    return true
}
function functionPolicy(errorFunction) {
    return !Tombstone.isTombstone(value) || errorFunction(value.error)
}
exports.RedisMap = RedisMap