const Redis = require('ioredis')
const config = require('config')
const _ = require('lodash')

let redis

function connect() {
  return new Promise((resolve, reject) => {
    redis = new Redis(config.get('redis'))
    redis.on('ready', () => {
      console.log('redis is ready')
      resolve(redis)
    })
    redis.on('close', () => console.log('redis is disconnected'))
    redis.on('error', (err) => reject(err))
  })
}

async function disconnect() {
  await redis.quit()
  redis = null
}

async function checkpoint(...dirs) {
  if (!isConnected()) throw new Error('redis is not connected')

  const now = Date.now()
  const method = 'set'
  const mode = 'ex'
  const ttl = 86400 // 1d

  const jobs = dirs.map((dir) => [method, dir, now, mode, ttl])
  const pipeline = redis.pipeline(jobs)

  const resp = await pipeline.exec()
  const errors = resp.map(([e]) => e).filter((e) => !_.isNil(e))

  if (errors.length) console.error(errors)
}

async function checkSync(...dirs) {
  if (!isConnected()) throw new Error('redis is not connected')

  const method = 'get'

  const jobs = dirs.map((dir) => [method, dir])
  const pipeline = redis.pipeline(jobs)

  const resp = await pipeline.exec()
  const vals = resp.map(([, val]) => !_.isNil(val))

  return Object.fromEntries(_.zip(dirs, vals))
}

function isConnected() {
  return !_.isNil(redis) && redis.status === 'ready'
}

module.exports = { connect, checkpoint, checkSync, disconnect }
