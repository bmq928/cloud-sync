const { EventEmitter } = require('events')
const config = require('config')
const autoBind = require('auto-bind')
const _ = require('lodash')

const {
  findChannels,
  findSyncDirs,
  findTargets,
  findCleanDirs,
} = require('./scan')
const { syncS3, cleanLocal } = require('./sync')

const FINISH_EVT = 'finish-sync'
const BATCH_SIZE = config.get('maxProcessPerBatch')
const SPAWN_TIME = config.get('processSpawnTime')

const mediator = new EventEmitter()
const now = Date.now()

class Processor {
  syncDirs = []
  channels = []
  curChannelTargets = []

  idxChannel = 0
  idxTarget = -1
  pendingTasks = 0

  constructor() {
    autoBind(this)
  }

  async init() {
    const channels = await findChannels()
    if (!channels.length) throw new Error('no channels found')

    this.channels = channels
    await this.feedSyncDirs(true)
  }

  sync() {
    return new Promise((resolve) => {
      for (let i = 0; i < BATCH_SIZE; ++i)
        setTimeout(this.consume, i * SPAWN_TIME)

      mediator.on(FINISH_EVT, async () => {
        if (!this.syncDirs.length) {
          const hasMore = await this.feedSyncDirs()
          const shouldStop = !hasMore && !this.pendingTasks
          
          if (shouldStop) return resolve()
          if(!hasMore) return
        }

        await this.consume()
      })
    })
  }

  async cleanup() {
    for (const channel of this.channels) {
      for (const target of await findTargets(channel)) {
        const dirs = await findCleanDirs(channel, target, now)
        await cleanLocal(...dirs)
      }
    }
  }

  async feedSyncDirs(fullBatch = false) {
    while (true) {
      const hasMore = this.increaseIdx()
      if (!hasMore) return hasMore

      const curChannel = this.channels[this.idxChannel]
      const isNewChannel = this.idxTarget === 0
      if (isNewChannel) this.curChannelTargets = await findTargets(curChannel)

      const curTarget = this.curChannelTargets[this.idxTarget]
      const syncDirs = await findSyncDirs(curChannel, curTarget, now)
      const shouldGetMore =
        syncDirs.length === 0 ||
        (fullBatch && this.syncDirs.length < BATCH_SIZE)

      this.syncDirs.push(...syncDirs)
      if (!shouldGetMore) return hasMore
    }
  }

  increaseIdx() {
    if (this.idxTarget < this.curChannelTargets.length - 1) {
      ++this.idxTarget
      return true
    }

    if (this.idxChannel < this.channels.length - 1) {
      ++this.idxChannel
      this.idxTarget = 0
      return true
    }

    return false
  }

  async consume() {
    try {
      const dir = this.syncDirs.pop()

      console.log(`start sync dir: ${dir}`)
      ++this.pendingTasks

      await syncS3(dir)
    } finally {
      --this.pendingTasks
      mediator.emit(FINISH_EVT)
    }
  }
}

module.exports = new Processor()
