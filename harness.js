const {connect} = require('./index')

function wait(ms, value) {
    return new Promise(done => setTimeout(() => done(value), ms))
}

async function main(args) {
    const cache = await connect()
    const m = cache.Map('test2:')
    let noclose = false;
    let peer;
    console.log('client:', cache.client)
    const commands = {
        help() {
            for (let command in commands) console.log(command)
        },
        async get(key) {
            console.log('get', key)
            console.log(await m.get(key))
        },
        async set(key, value, delay) {
            console.log('set', key, delay)
            await m.set(key, wait(+delay, value))
            console.log('set done')
        },
        flush() { cache.redis.flushdb() },
        close() { cache.close() },
        noclose() { noclose = true },
        async wait(delay) {
            console.log('waiting', delay)
            console.log(await wait(+delay, 'done'))
        },
        async sub(channel) {
            console.log('#open=', await cache.sub.subscribe(channel))
        },
        async pub(channel, message) {
            await cache.redis.publish(channel, message)
        },
        async sendclient(channel) {
            await cache.redis.publish(channel, cache.client)
        },
        async getpeer(channel) {
            let resolve, promise = new Promise(r => resolve = r)
            cache.sub.on('message', (ch, message) => {
                if (ch == channel) {
                    peer = message
                    console.log('peer:', peer)
                    resolve()
                }
            })
            await cache.sub.subscribe(channel)
            await promise
        },
        async ping(who) {
            if (who == 'peer') who = peer
            console.log('pinging', who)
            try {
                await cache.ping(who, true)
                console.log('succeeded')
            } catch(e) {
                console.error('ping failed', e)
            }
        },
        async onmsg() {
            cache.sub.on('message', (ch, msg) => console.log('MSG ', ch, msg))
            cache.redis.on('message', (ch, msg) => console.log('MSG(redis) ', ch, msg))        
        }
    }
    while (args.length) {
        let cmdname = args.shift()
        let cmd = commands[cmdname]
        if (!cmd) {
            console.log('?', cmdname)
            continue
        }
        let cmdargs = args.splice(0, cmd.length)
        console.log('>', cmd.name, ...cmdargs)
        await cmd(...cmdargs)
        console.log('/', cmd.name)
    }
    if (!noclose) cache.close()
}

let args = process.argv.slice(2)
if (args.length === 0) args = ['help']
main(args)
