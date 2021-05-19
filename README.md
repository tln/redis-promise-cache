# Redis promise cache

This presents a Map interface over redis that allows promises. The intended use is for a cache where missing items are fetched. Then when the fetch is started, the promise for the fetch is put into the cache,. When the fetch completes, the cache is updated.

We don't assume the promise will be frequested by the same process that fulfilled it; this module implements a way for other processes to request and also verify that the fulfilling process is alive.

## Usage

```
const connect = require('redis-promise-cache')

const conn = connect({ ... ioredis options ... })
const cache = conn.Map('mapkey', {writeback: 'noErrors'|'all'|function})
```

The returned map has these methods:
```
async get(key)
async set(key, value)
async delete(key)
```

## Writing back errors

If Promise is set, and ends up rejecting, we may or may not want to write that error to the cache. The default is to not write errors.

You may wish to write an error if the result is a 400 (won't be resolved without something changing), but not on a 500 or other error that could work again in the future.

## Recovery from dead processes

If the process fulfilling a promise dies, other processes that are trying to retrieve that promise will ping the process through pubsub and detect that it is dead. The `get` will then return `null` as if the key was not present, and the promise is removed from the cache.
