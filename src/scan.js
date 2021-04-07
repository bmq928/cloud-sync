const fs = require('fs')
const path = require('path')
const config = require('config')
const dayjs = require('dayjs')
const { ObjectID } = require('mongodb')
const _ = require('lodash')

const ROOT_FOLDER = config.get('rootLocal')

async function findChannels() {
  const dirs = await fs.promises.readdir(ROOT_FOLDER)
  const dirStats = await Promise.all(
    dirs
      .map((item) => path.join(ROOT_FOLDER, item))
      .map((dir) => fs.promises.stat(dir))
  )
  const channels = _.zip(dirs, dirStats)
    .filter(([dir, stat]) => stat.isDirectory() && ObjectID.isValid(dir))
    .map(([dir]) => dir)
  return channels
}

async function findTargets(channel) {
  const dir = path.join(ROOT_FOLDER, channel)
  const targets = await fs.promises.readdir(dir)
  return targets
}

async function findTargetType(channel, target) {
  const dir = path.join(ROOT_FOLDER, channel, target)
  const children = await fs.promises.readdir(dir)
  const sample = _.last(children)
  const type =
    parseInt(sample) > 2000010101 // 2000-01-01 01h
      ? 'dash'
      : 'hls'
  return type
}

async function findTimeDirs(channel, target, timestamp) {
  const type = await findTargetType(channel, target)
  const targetDir = path.join(ROOT_FOLDER, channel, target)

  const timeRange = config.get('timeRange')
  const cur = dayjs(timestamp)
  const folders = _.range(timeRange).map((h) =>
    cur.subtract(h, 'hour').format('YYYYMMDDHH')
  )

  if (type === 'dash')
    return folders.map((folder) => path.join(targetDir, folder))

  const profiles = await fs.promises.readdir(targetDir)
  return profiles
    .map((profile) =>
      folders.map((folder) => path.join(targetDir, profile, folder))
    )
    .flat()
}

async function findSyncDirs(channel, target, timestamp) {
  const dirs = await findTimeDirs(channel, target, timestamp)
  const isExists = await Promise.all(
    dirs.map((dir) =>
      fs.promises
        .access(dir, fs.constants.F_OK)
        .then(() => true)
        .catch(() => false)
    )
  )
  const syncDirs = _.zip(dirs, isExists)
    .filter(([, isExist]) => isExist)
    .map(([dir]) => dir)
  return syncDirs
}

module.exports = { findChannels, findTargets, findTargetType, findSyncDirs }
