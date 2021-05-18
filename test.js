const {RedisMap, close, redis, sub, ping, client} = require('./index')

function wait(ms, value) {
    return new Promise(done => setTimeout(() => done(value), ms))
}
async function test() {
    const m = new RedisMap('foo:')
    await m.set('foo', 'bar')
    console.log(await m.get('foo'))
    console.log(await m.get('foo2'))
    await m.set('foo', wait(1000, 'delayed'))
    console.log(await m.get('foo'))
}

async function testWithArgs(args) {
    const m = new RedisMap('test2:')
    let noclose = false;
    let peer;
    console.log('client:', client)
    const commands = {
        async get(key) {
            console.log('get', key)
            console.log(await m.get(key))
        },
        async set(key, value, delay) {
            console.log('set', key, delay)
            await m.set(key, wait(+delay, value))
            console.log('set done')
        },
        close() { close() },
        noclose() { noclose = true },
        async wait(delay) {
            console.log('waiting', delay)
            console.log(await wait(+delay, 'done'))
        },
        async sub(channel) {
            console.log('#open=', await sub.subscribe(channel))
        },
        async pub(channel, message) {
            await redis.publish(channel, message)
        },
        async sendclient(channel) {
            await redis.publish(channel, client)
        },
        async getpeer(channel) {
            let resolve, promise = new Promise(r => resolve = r)
            sub.on('message', (ch, message) => {
                if (ch == channel) {
                    peer = message
                    console.log('peer:', peer)
                    resolve()
                }
            })
            await sub.subscribe(channel)
            await promise
        },
        async ping(who) {
            if (who == 'peer') who = peer
            console.log('pinging', who)
            try {
                await ping(who)
                console.log('succeeded')
            } catch(e) {
                console.error('ping failed', e)
            }
        },
        async onmsg() {
            sub.on('message', (ch, msg) => console.log('MSG ', ch, msg))
            redis.on('message', (ch, msg) => console.log('MSG(redis) ', ch, msg))        
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
    if (!noclose) close()
}


if (process.argv[2]) {
    testWithArgs(process.argv.slice(2))
} else {
    test().then(console.log('Done'))
}
