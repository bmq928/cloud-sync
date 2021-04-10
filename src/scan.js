const fs = require('fs')
const path = require('path')
const config = require('config')
const dayjs = require('dayjs')
const { ObjectID } = require('mongodb')
const _ = require('lodash')
const { checkSync } = require('./db')

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

async function findTimeDirs(channel, target, from, range) {
  const type = await findTargetType(channel, target)
  const targetDir = path.join(ROOT_FOLDER, channel, target)

  const pivot = dayjs.utc(from)
  const times = _.range(range).map((h) =>
    pivot.subtract(h + 1, 'hour').format('YYYYMMDDHH')
  )

  if (type === 'dash') {
    const timeDirs = times.map((time) => path.join(targetDir, time))
    const timeProfiles = await Promise.all(
      timeDirs.map((dir) => fs.promises.readdir(dir))
    )
    return _.zip(timeDirs, timeProfiles)
      .map(([timeDir, profiles]) =>
        profiles.map((profile) => path.join(timeDir, profile))
      )
      .flat()
  }

  //hls
  const profiles = await fs.promises.readdir(targetDir)
  return profiles
    .map((profile) => times.map((time) => path.join(targetDir, profile, time)))
    .flat()
}

async function findSyncDirs(channel, target, from) {
  const range = config.get('timeRange.sync')
  const dirs = await findTimeDirs(channel, target, from, range)

  const isExists = await Promise.all(
    dirs.map((dir) =>
      fs.promises
        .access(dir, fs.constants.F_OK)
        .then(() => true)
        .catch(() => false)
    )
  )
  const existedDirs = _.zip(dirs, isExists)
    .filter(([, isExist]) => isExist)
    .map(([dir]) => dir)

  const isSyncs = await checkSync(...existedDirs)
  const syncDirs = _.zip(existedDirs, isSyncs)
    .filter(([, isSync]) => !isSync)
    .map(([dir]) => dir)

  return syncDirs
}

async function findCleanDirs(channel, target, from) {
  const preserveRange = config.get('timeRange.preserve')
  const preserveDirs = await findTimeDirs(channel, target, from, preserveRange)

  const syncDirs = await findSyncDirs(channel, target, from)

  const cleanDirs = syncDirs.filter((dir) => preserveDirs.includes(dir))
  return cleanDirs
}

module.exports = {
  findChannels,
  findTargets,
  findTargetType,
  findSyncDirs,
  findCleanDirs,
}
