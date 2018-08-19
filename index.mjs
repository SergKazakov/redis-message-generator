#!/usr/bin/env node --experimental-modules
import yargs from "yargs"
import Redis from "ioredis"

const { redisUrl } = yargs.usage("Usage: $0 --redis-url").option("redis-url", {
  type: "string",
  default: "redis://localhost:6379",
}).argv

const errorHandler = fn => (...args) => fn(...args).catch(console.log)

const PUBLISHER_KEY = "publisher"

const SUBSCRIBERS_KEY = "subscribers"

const MESSAGE_CHANNEL = "queue"

const PROCESS_ID = process.pid
;(async () => {
  const redis = new Redis(redisUrl)

  let pub = null

  let pubIntervalId = null

  let sub = null

  const publisherExists = await redis.exists(PUBLISHER_KEY)

  if (publisherExists) {
    sub = new Redis(redisUrl)

    sub.subscribe(
      [MESSAGE_CHANNEL, PROCESS_ID],
      errorHandler(async () => {
        await redis.rpush(SUBSCRIBERS_KEY, PROCESS_ID)

        console.log(`Subscriber ${PROCESS_ID} is listening`)
      }),
    )

    sub.on(
      "message",
      errorHandler(async (channel, message) => {
        if (channel === MESSAGE_CHANNEL) {
          return console.log(message)
        }

        await redis.lrem(SUBSCRIBERS_KEY, 0, PROCESS_ID)

        sub.unsubscribe([MESSAGE_CHANNEL, PROCESS_ID])

        pub = new Redis()

        await redis.set(PUBLISHER_KEY, PROCESS_ID)

        console.log(`Publisher ${PROCESS_ID} is starting`)

        pubIntervalId = setInterval(
          errorHandler(() => pub.publish(MESSAGE_CHANNEL, PROCESS_ID)),
          500,
        )
      }),
    )
  } else {
    pub = new Redis()

    await redis.set(PUBLISHER_KEY, PROCESS_ID)

    console.log(`Publisher ${PROCESS_ID} is starting`)

    pubIntervalId = setInterval(
      errorHandler(() => pub.publish(MESSAGE_CHANNEL, PROCESS_ID)),
      500,
    )
  }

  process.on(
    "SIGINT",
    errorHandler(async () => {
      if (pub) {
        await redis.del(PUBLISHER_KEY)

        clearInterval(pubIntervalId)

        console.log(`Publisher ${PROCESS_ID} finished`)

        const [firstSubscriber] = await redis.lrange(SUBSCRIBERS_KEY, 0, 1)

        if (firstSubscriber) {
          redis.publish(firstSubscriber, "start")
        }
      }

      if (sub) {
        await redis.lrem(SUBSCRIBERS_KEY, 0, PROCESS_ID)
      }

      await Promise.all([
        redis.quit(),
        ...(pub ? [pub.quit()] : []),
        ...(sub ? [sub.quit()] : []),
      ])
    }),
  )
})().catch(console.log)
