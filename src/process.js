const config = require('config')
const _ = require('lodash')
const { findTargets, findSyncDirs, findChannels, findCleanDirs } = require('./scan')
const { syncS3, cleanLocal } = require('./sync')

async function run() {
  const now = Date.now()
  const batchSize = config.get('maxProcessPerBatch')

  let totalSyncDirs = []

  for (const channel of await findChannels()) {
    for (const target of await findTargets(channel)) {
      const syncDirs = await findSyncDirs(channel, target, now)
      totalSyncDirs.push(...syncDirs)

      if (totalSyncDirs.length < batchSize) continue

      const leftover = await batchProcess(totalSyncDirs, batchSize)
      totalSyncDirs = leftover

      const cleanDirs = await findCleanDirs(channel, target, now)
      await cleanLocal(...cleanDirs)
    }
  }

  await batchProcess(totalSyncDirs, batchSize, true)
}

async function batchProcess(dirs, batchSize, force = false) {
  const batches = _.chunk(dirs, batchSize)
  for (const batch of batches) {
    const shouldProcess = batch.length === batchSize || force
    if (shouldProcess) await spawnGradually(batch)
  }

  const leftover =
    force || _.last(batches).length === batchSize ? [] : _.last(batches)
  return leftover
}

async function spawnGradually(dirs) {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
  const duration = config.get('processSpawnTime')

  return await Promise.all(
    dirs.map(async (dir, idx) => {
      const seconds = idx * duration

      console.log(`start syncS3 dir: ${dir}`)
      await sleep(seconds)
      await syncS3(dir)
    })
  )
}

module.exports = { run }
