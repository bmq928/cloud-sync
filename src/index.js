const dayjs = require('dayjs')
const { connect, disconnect } = require('./db')
const { run } = require('./process')

const startTime = dayjs()
dayjs.extend(require('dayjs/plugin/utc'))

Promise.resolve()
  .then(() => console.log(`job start at ${startTime}`))
  .then(connect)
  .then(run)
  .then(disconnect)
  .finally(() => console.log(`job take ${dayjs().diff(startTime, 'second')}s`))
