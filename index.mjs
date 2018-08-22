#!/usr/bin/env node --experimental-modules
import yargs from "yargs"
import Redis from "ioredis"

const { redisUrl, getErrors } = yargs
  .usage("Usage: $0")
  .option("redisUrl", {
    type: "string",
    default: "redis://localhost:6379",
  })
  .option("getErrors", {
    type: "boolean",
    default: false,
  }).argv

const MASTER_KEY = "master"

const MESSAGES_KEY = "messages"

const ERRORS_KEY = "errors"

const PROCESS_ID = process.pid

const master = async ({ redisClient, pid, isFirstCall = false }) => {
  const currentMaster = await redisClient.get(MASTER_KEY)

  if (currentMaster === null || Number(currentMaster) !== pid) {
    return
  }

  if (isFirstCall) {
    console.log(`Master ${pid} is sending messages`)
  }

  const message = Math.random()

  await redisClient.rpush(MESSAGES_KEY, message)

  console.log("send", message)

  setTimeout(master, 500, { redisClient, pid })
}

const slave = async ({ redisClient, pid, isFirstCall = false }) => {
  const currentMaster = await redisClient.get(MASTER_KEY)

  if (currentMaster === null || Number(currentMaster) === pid) {
    return
  }

  if (isFirstCall) {
    console.log(`Slave ${pid} is getting messages`)
  }

  const message = await redisClient.rpop(MESSAGES_KEY)

  if (message) {
    console.log("get", message)

    if (Math.random() < 0.05) {
      await redisClient.rpush(ERRORS_KEY, message)

      console.log(`Error ${message} was found`)
    }
  }

  setTimeout(slave, 500, { redisClient, pid })
}
;(async () => {
  const redis = new Redis(redisUrl)

  if (getErrors) {
    const [[, errors]] = await redis
      .multi()
      .lrange(ERRORS_KEY, 0, -1)
      .del(ERRORS_KEY)
      .exec()

    if (errors.length === 0) {
      console.log("Errors are not found")
    } else {
      errors.forEach(err => console.log(err))
    }

    return process.exit(0)
  }

  redis.on("ready", () => redis.config("SET", "notify-keyspace-events", "KEA"))

  const expirationListener = new Redis(redisUrl)

  expirationListener.subscribe("__keyevent@0__:expired")

  expirationListener.on("message", async () => {
    try {
      console.log("Master has been expired")

      if ((await redis.set(MASTER_KEY, PROCESS_ID, "EX", 10, "NX")) === "OK") {
        return master({
          redisClient: redis,
          pid: PROCESS_ID,
          isFirstCall: true,
        })
      }

      await slave({ redisClient: redis, pid: PROCESS_ID, isFirstCall: true })
    } catch (err) {
      console.log(err)
    }
  })

  if ((await redis.set(MASTER_KEY, PROCESS_ID, "EX", 10, "NX")) === "OK") {
    return master({
      redisClient: redis,
      pid: PROCESS_ID,
      isFirstCall: true,
    })
  }

  await slave({ redisClient: redis, pid: PROCESS_ID, isFirstCall: true })
})().catch(console.log)
