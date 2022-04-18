import nats, { createQueueGroup, setupStream, closeClient } from './nats.js'
import { JSONCodec, consumerOpts, createInbox, toJsMsg } from 'nats'

const jsonCodec = JSONCodec()

let simpleSub = async () => {
  let process = async sub => {
    for await (const m of sub) {
      console.log(
        `[${sub.getProcessed()}]: ${JSON.stringify(jsonCodec.decode(m.data))}`
      )
    }
  }

  try {
    let nc = await nats()
    console.log(`connected to ${nc.getServer()}`)

    let sub = nc.subscribe('hello')
    process(sub).catch(console.trace)

    setInterval(
      () => nc.publish('hello', jsonCodec.encode({ timeStamp: new Date() })),
      1000
    )

    // await closeClient(nc)
  } catch (err) {
    console.trace(err)
  }
}

/*
creates 1 or more NATS subscriptions and distributes evenly messages across each subscription/consumer
increase consumer count to increase throughput
*/
let workerQueue = async () => {
  let consumerCount = 3
  try {
    await createQueueGroup(
      'testQueue',
      'testSubject',
      consumerCount,
      async data => {
        // simulate work
        let result = await new Promise(resolve => {
          setTimeout(() => resolve(data), 500)
        })
        return result
      }
    )

    let nc = await nats()
    console.log(`connected to ${nc.getServer()}`)

    setInterval(
      () =>
        nc.publish('testSubject', jsonCodec.encode({ timeStamp: new Date() })),
      100
    )
    // await closeClient(nc)
  } catch (err) {
    console.trace(err)
  }
}

let consumePushSubscription = async (sub, name) => {
  let msgCount = 0
  for await (let msg of sub) {
    let data = jsonCodec.decode(msg.data)
    let {
      info: { redelivered, redeliveryCount },
    } = msg

    console.log(
      new Date(),
      `Processing on ${name}, redelivered: ${redelivered}, redeliveryCount: ${redeliveryCount}, data: ${JSON.stringify(
        data
      )}`
    )

    // if initial attempt, retry in some time (basically fake a processing error)
    if (!redelivered) {
      // delayed retry without blocking from consuming the next message
      // (can be linear or exponential based on logic on redeliveryCount)
      // !!! This allows for delayed retries without blocking the processing of the next message
      await msg.nak(5000)
    } else {
      // simulate slow message once every 100 messages
      let delayProcessingMs = msgCount % 100 === 0 ? 5000 : 0
      await new Promise(resolve => {
        setTimeout(async () => {
          let ack = await msg.ack()
          resolve(ack)
          msgCount++
        }, delayProcessingMs)
      })
    }
  }
}

let streamWorkerPushQueue = async () => {
  let nc = await nats()
  let jetStream = nc.jetstream()
  let jsm = await nc.jetstreamManager()

  let streamName = 'testStream'
  await setupStream(nc, {
    name: streamName,
    // deleteStreamIfExist: true,
    // deleteConsumers: true,
  })

  let allConsumers = await jsm.consumers.list(streamName).next()
  let consumerName = 'testStream-consumer-push'
  console.log(allConsumers.find(({ name }) => name === consumerName))

  // increase number of subscriptions to horizontally scale
  let numberOfSubs = 5
  // number of attempts per message
  let maxDeliver = 5
  for (let i = 1; i <= numberOfSubs; i++) {
    let subOptions = consumerOpts()
    subOptions.manualAck()
    subOptions.durable(consumerName)
    subOptions.queue('testStream-consumer-queue')
    subOptions.deliverTo(createInbox())
    subOptions.maxDeliver(maxDeliver)
    // push subscriber that enforces a queue with acknowledgements
    let sub = await jetStream.subscribe('testStream.*', subOptions)
    // a subscription is an async generator
    consumePushSubscription(sub, `sub-${i}`, maxDeliver).catch(console.error)
  }

  setInterval(
    () =>
      jetStream.publish(
        'testStream.doStuff',
        jsonCodec.encode({ timeStamp: new Date() })
      ),
    100
  )
}

let consumePullSubscription = async (pullSub, name) => {
  pullSub.messagesInProgress = 0
  for await (let msg of pullSub) {
    pullSub.messagesInProgress++
    console.log('messagesInProgress', pullSub.messagesInProgress)
    let data = jsonCodec.decode(msg.data)
    let {
      info: { redelivered, redeliveryCount },
    } = msg

    console.log(
      new Date(),
      `Processing on ${name}, redelivered: ${redelivered}, redeliveryCount: ${redeliveryCount}, data: ${JSON.stringify(
        data
      )}`
    )

    new Promise((resolve, reject) => {
      // if initial attempt, retry in some time (basically fake a processing error)
      if (!redelivered) {
        pullSub.messagesInProgress--
        // delayed retry without blocking from consuming the next message
        // (can be linear or exponential based on logic on redeliveryCount)
        // !!! This allows for delayed retries without blocking the processing of the next message
        msg.nak(5000)
        resolve()
      } else {
        // simulate slow message once every 50 messages processed
        let delayProcessingMs = pullSub.getProcessed() % 50 === 0 ? 5000 : 0
        new Promise(resolveTimeout => {
          setTimeout(async () => {
            pullSub.messagesInProgress--
            let ack = await msg.ackAck()
            resolveTimeout(ack)
          }, delayProcessingMs)
        })
          .catch(reject)
          .then(resolve)
      }
    })
  }
}

let streamWorkerPullQueue = async () => {
  let nc = await nats()
  let jetStream = nc.jetstream()
  let jsm = await nc.jetstreamManager()

  let streamName = 'testStream'
  await setupStream(nc, {
    name: streamName,
    // deleteStreamIfExist: true,
    // deleteConsumers: true,
  })

  let allConsumers = await jsm.consumers.list(streamName).next()
  let consumerName = 'testStream-consumer-pull'
  console.log(allConsumers.find(({ name }) => name === consumerName))

  // increase batch size and/or numberOfSubs to horizontally scale
  let batch = 5
  let numberOfSubs = 5
  // number of attempts per message
  let maxDeliver = 5
  let pullIntervalMs = 1000

  let subOptions = consumerOpts()
  subOptions.durable('testStream-consumer-pull')
  subOptions.maxDeliver(maxDeliver)
  // disable consumer max for pending messages since using a pull mechanism
  subOptions.maxAckPending(-1)

  let pullSubs = []
  for (let i = 1; i <= numberOfSubs; i++) {
    // pull subscriber
    let pullSub = await jetStream.pullSubscribe('testStream.*', subOptions)
    consumePullSubscription(pullSub, `pullSub-${i}`).catch(console.error)
    pullSubs.push(pullSub)
  }

  // trigger pull based on defined interval
  setInterval(() => {
    let i = 0
    for (let pullSub of pullSubs) {
      let name = `pullSub-${++i}`
      // make sure we are never processing more messages than what the batch size is concurrently
      // "messagesInProgress" is custom-made not part of JetStream (need to see if there is a similar built-in alternative)
      let nextBatch = batch - pullSub.messagesInProgress
      if (nextBatch > 0) {
        console.log(`[${name}] Pulling ${nextBatch} message(s)`)
        pullSub.pull({ batch, expires: pullIntervalMs })
      } else {
        console.log(`[${name}] Skip pull, subscription is busy processing`)
      }
    }
  }, pullIntervalMs)

  // publish some messages to see them consumed
  setInterval(
    () =>
      jetStream.publish(
        'testStream.doStuff',
        jsonCodec.encode({ timeStamp: new Date() })
      ),
    100
  )
}

// basic NATS (no persistence) pub-sub a.k.a "Hello World"
// simpleSub().catch(console.trace)

// basic NATS (no persistence) pub-sub using a queue group to horizontally scale processing of messages
// workerQueue().catch(console.trace)

// JetStream push queue consumer using a queue group to horizontally scale processing of messages
// Main Pro: each subscription automatically gets messages from the server to process
// Main Con: the total combined subscription(s) to a push consumer can process concurrently at most are controlled by the server https://docs.nats.io/nats-concepts/jetstream/consumers#maxackpending
// streamWorkerPushQueue().catch(console.trace)

// JetStream pull queue consumer
// Main Pro: https://docs.nats.io/nats-concepts/jetstream/consumers#maxackpending can be disabled and the horizontal scaling of a pull consumer is only determined
// by how many application subscriptions are pulling from this consumer simultaneously (basically there is no server constraint on max pending messages)
// Main Con: each subscription has to manually pull messages to process (and has to track how many are currently being processed every time it pulls,
// if we need to enforce not to process more than a certain number on a subscription in parallel)
streamWorkerPullQueue().catch(console.trace)
