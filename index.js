const Redis = require("ioredis");
const util = require('util')
const debug = require('debug')('redis-promise-cache')

class Connection {
    constructor(...args) {
        this.redis = new Redis(...args)
        this.sub = this.redis.duplicate()
    }
}

const redis = new Redis();
const sub = redis.duplicate()
function close() {
    redis.disconnect()
    sub.disconnect()
}
exports.redis = redis
exports.sub = sub
exports.close = close

// -- Channel handling
function drop(ch, handler) { return true; }
function logHandler(string) {
    return (channel, message) => debug(string, channel, message)
}
function listHandler(unhandled=drop) {
    const handlers = []
    const func = (channel, message) => {
        for (const handler of handlers) {
            if (handler(channel, message)) {
                debug('handled', handler, channel, message);
                return
            }
        }
        func.unhandled(channel, message)
    }
    func.unhandled = unhandled
    func.push = handler => handlers.push(handler)
    return func
}
const messageHandler = listHandler(logHandler('unhandled'))
messageHandler.push(logHandler('message')) // For debugging
sub.on('message', messageHandler)

// --- An identifier for this client
const client = 'client_'+Date.now()+'_'+process.pid
exports.client = client
function clientHandler(ch, message) {
    if (ch === client) {
        // client commands -- cancellation, ping etc
        debug('got client message:', message)
        try {
            const args = message.split(' '), name = args.shift(), command = clientCommands[name];
            if (!command) {
                debug('unknown command', name)
                return false
            }
            command(...args)
        } catch(e) {
            debug('error in client handler', e)
        }
        return true
    }
    return false
}
messageHandler.push(clientHandler)
sub.subscribe(client)

const clientCommands = {
    ping(peer) {
        // peer is pinging us. send back a pong
        pong(peer)
    },
    pong(peer) {
        // Our ping got back a pong
        pings[peer]()
    }
}

const pings = {}
function ping(peer, timeout=2000) {
    debug('ping', peer)
    redis.publish(peer, 'hello')
    return new Promise((resolve, reject) => {
        // TODO racy!
        debug('in promise')
        const timer = setTimeout(() => reject('no pong from '+peer), timeout);
        pings[peer] = () => (clearTimeout(timer), resolve())
        redis.publish(peer, 'ping '+client)
    })
}
function pong(peer) {
    redis.publish(peer, 'pong '+client)
}
exports.ping = ping

// --- Channel -> Callbacks
const callbacks = {}  // channel: callbacks
function handleCallbacks(channel, message) {
    const list = callbacks[channel];
    if (!list) return false;

    for (const cb of list) {
        try {
            cb(message)
        } catch(e) {
            debug('error in callback', cb, message, channel)
        }
    }
    delete callbacks[channel]
    sub.unsubscribe(channel)
    return true;
}
messageHandler.push(handleCallbacks)
function subOnce(channel, callback) {
    let list = callbacks[channel];
    if (list) {
        list.push(callback)
    } else {
        callbacks[channel] = [callback]
        sub.subscribe(channel)
    }
}

// --- Promise tokens
const promises = {length: 0}
function isPromiseToken(s) {
    return s.startsWith('promise-')
}
function storePromise(value, callback) {
    const id = promises.length++;
    promises[id] = value
    const token = `promise-${client}-${id}`
    resolvePromise(token, value, callback)
    return token
}
async function resolvePromise(token, promise, callback) {
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
    redis.publish(token, redisString)
}

function waitForPromise(token) {
    let [_, promiseClient, id] = token.split('-')
    if (promiseClient == client) {
        // TODO Can we delete it now?
        return promises[id];
    } else {
        debug('waiting for', promiseClient, id)
        ping(promiseClient)
        return new Promise(resolve => 
            subOnce(token, message => resolve(fromRedis(message)))
        )
    }
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
function toRedis(value, callback) {
    if (util.types.isPromise(value)) {
        debug('storing a promise')
        return storePromise(value, callback)
    } else if (value instanceof Tombstone) {
        return value.toRedis()
    }
    return JSON.stringify(value)
}
function fromRedis(data) {
    if (data === null) return null
    if (Tombstone.isTombstone(data)) return Tombstone.dataToTombstone(data)
    if (isPromiseToken(data)) return waitForPromise(data)
    try {
        return JSON.parse(data)
    } catch(e) {
        console.error('Error parsing JSON', e, data)
        throw e;
    }
}


// -- redis map
class RedisMap {
    constructor(hkey, {writeback='noErrors'}={}) {
        this.hkey = hkey
        this.writebackPolicy = writebackPolicy(writeback)
    }
    get(key) {
        return redis.hget(this.hkey, key).then(data => fromRedis(data))
    }
    set(key, value) {
        const data = toRedis(value, value => this.writeback(key, value))
        return redis.hset(this.hkey, key, data)
    }
    writeback(key, value) {
        // different behavior on errors
        if (this.writebackPolicy(value, key)) {
            debug('writing back', key, value)
            return redis.hset(this.hkey, key, toRedis(value))
        } else {
            debug('not writing back', key, value)
            // Delete the promise
            return redis.hdel(this.hkey, key)
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