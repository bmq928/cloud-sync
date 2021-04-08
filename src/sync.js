const { spawn } = require('child_process')
const dayjs = require('dayjs')
const _ = require('lodash')
const path = require('path')
const config = require('config')

const ROOT_LOCAL = config.get('rootLocal')
const ROOT_S3 = config.get('rootS3')

function sync(localDir) {
  const startTime = dayjs()
  const cp = () => exec(genScript('put', localDir))
  const sync = () => exec(genScript('sync', localDir))
  const rm = () => exec(`rm -rf ${localDir}`)

  return Promise.resolve()
    .then(cp)
    .catch(sync)
    .then(rm)
    .catch((err) => console.log(err))
    .finally(() =>
      console.log(
        `finished sync ${localDir} in ${dayjs().diff(startTime, 'second')}s`
      )
    )
}

function genScript(command, localDir) {
  const standardLocalDir = path.join(localDir, '/')
  const s3Dir = standardLocalDir.replace(ROOT_LOCAL, ROOT_S3)
  const s3Uri = `s3:/${s3Dir}` //ROOT_S3 start with /
  const binary = config.get('executor')

  return `${binary} ${command} ${standardLocalDir} ${s3Uri} --recursive`
}

function exec(script) {
  return new Promise((resolve, reject) => {
    const [cmd, ...args] = _.compact(script.split(' '))
    const ps = spawn(cmd, args)

    if(config.get('executorLog')) {
      ps.stdout.on('data', data => console.log('output: ', data.toString()))
      ps.stderr.on('data', data => console.log('err: ', data.toString()))
    }

    ps.on('close', (code, signal) => {
      console.log({ script, code, signal, event: 'close' })

      if (code === 0) return resolve()
      reject({ code, signal })
    })

    ps.on('exit', (code, signal) => {
      console.log({ script, code, signal, event: 'exit' })

      if (code === 0) return resolve()
      reject({ code, signal })
    })

    ps.on('error', (err) => {
      console.log({ script, event: 'error', err })
      reject(err)
    })
  })
}

module.exports = { sync }
