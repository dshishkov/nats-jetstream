import {
  connect,
  DiscardPolicy,
  JSONCodec,
  RetentionPolicy,
  StorageType,
} from 'nats'

let jsonCodec = new JSONCodec()

// docker run -p 4222:4222 -p 8222:8222 -p 6222:6222 --name nats-server -ti nats:latest -js -sd /home/dimitar/jetstream-storage --auth aToK3n
export let natsConfig = {
  maxReconnectAttempts: 50,
  // debug: true,
  // verbose: true
}

let natsConnect = (config = natsConfig) => connect(config)
export default natsConnect

// close nats client helper
export let closeClient = async (nc, { drain = true } = {}) => {
  if (!nc) {
    throw Error('NATS client missing')
  }
  let done = nc.closed()
  await (drain ? nc.drain() : nc.close())
  // check if the close was OK
  let err = await done
  if (err) {
    throw err
  }
}

export let createQueueGroup = async (
  queue = 'queue',
  subject = 'test',
  consumerCount = 1,
  process = x => x
) => {
  let connections = []

  for (let i = 1; i <= consumerCount; i++) {
    let name = `${queue}-${i}`
    let nc = await natsConnect({ ...natsConfig, name })

    nc.closed().then(err => {
      if (err) {
        console.error(`service ${name} exited because of error: ${err.message}`)
      }
    })

    // create a subscription - note the option for a queue, if set
    // any client with the same queue will be a member of the group.
    let sub = nc.subscribe(subject, { queue })
    let processQueueMessage = async sub => {
      for await (let message of sub) {
        let data = jsonCodec.decode(message.data)
        let result = await process(data)
        console.log(
          `[${name}]: #${sub.getProcessed()} result ${JSON.stringify(result)}`
        )
      }
    }
    processQueueMessage(sub).catch(console.error)
    console.log(`${nc.options.name} is listening for messages...`)
    connections.push(nc)
  }
}

export let setupStream = async (
  nc,
  // stream config
  {
    name = 'testStream',
    retention = RetentionPolicy.Limits,
    subjects = [`${name}.*`],
    storage = StorageType.File,
    // custom options for this fn, not related to the built-in JetStream config options
    deleteStreamIfExist = false,
    purge,
    deleteConsumers = false,
  } = {}
) => {
  let jsm = await nc.jetstreamManager()
  let streamConfig = {
    name,
    subjects,
    retention,
    storage,
    // when the retention limits are reached on the stream, deny publishing new messages
    discard: DiscardPolicy.New,
  }
  let streamInfo

  try {
    await jsm.streams.info(name)

    if (deleteStreamIfExist) {
      await jsm.streams.delete(name)
    }
    await jsm.streams.update(name, streamConfig)

    if (purge) {
      await jsm.streams.purge(name, purge)
    }
    streamInfo = await jsm.streams.info(name)
  } catch (err) {
    if (!/stream not found/.test(err.message)) {
      throw err
    }
    await jsm.streams.add(streamConfig)
    streamInfo = await jsm.streams.info(name)
  }

  if (deleteConsumers) {
    // cleanup any other consumers
    let allConsumers = await jsm.consumers.list(name).next()
    for (let consumer of allConsumers) {
      await jsm.consumers.delete(name, consumer.name)
    }
  }

  return streamInfo
}
