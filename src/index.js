const dayjs = require('dayjs')
const db = require('./db')
const processor = require('./processor')

const startTime = dayjs()
dayjs.extend(require('dayjs/plugin/utc'))

Promise.resolve()
  .then(() => console.log(`job start at ${startTime}`))
  .then(db.connect)
  .then(processor.init)
  .then(processor.sync)
  .then(processor.cleanup)
  .then(db.disconnect)
  .finally(() => console.log(`job take ${dayjs().diff(startTime, 'second')}s`))
