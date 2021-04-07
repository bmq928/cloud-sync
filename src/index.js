const config = require('config')
const _ = require('lodash')
const { promisify } = require('util')
const { findTargets, findSyncDirs, findChannels } = require('./scan')
const { sync } = require('./sync')

main()
async function main() {
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
  const sleep = promisify(setTimeout)
  const startTime = config.get('startTime')

  return await Promise.all(
    dirs.map(async (dir, idx) => {
      const seconds = idx * startTime

      console.log(`sync dir: ${dir}`)
      await sleep(seconds)
      await sync(dir)
    })
  )
}
