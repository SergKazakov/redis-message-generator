#!/usr/bin/env node
import { randomUUID } from "node:crypto"
import { parseArgs } from "node:util"

import { Redis } from "ioredis"

const {
  values: { redisUrl, getErrors },
} = parseArgs({
  options: {
    redisUrl: { type: "string", default: "redis://localhost:6379" },
    getErrors: { type: "boolean" },
  },
})

const redis = new Redis(redisUrl)

const ERRORS_KEY = "errors"

if (getErrors) {
  const response = await redis
    .multi()
    .lrange(ERRORS_KEY, 0, -1)
    .del(ERRORS_KEY)
    .exec()

  await redis.quit()

  if (response) {
    const [[, errors]] = response

    if (Array.isArray(errors)) {
      for (const error of errors) console.log(error)
    } else {
      console.log("Errors are not found")
    }
  }

  process.exit(0)
}

const MASTER_KEY = "master"

const MESSAGES_KEY = "messages"

let role: string

let timeout: ReturnType<typeof setTimeout>

const beMaster = async () => {
  if (role !== "master") {
    console.log(`Master ${process.pid} is sending messages`)

    role = "master"
  }

  const message = randomUUID()

  await redis.rpush(MESSAGES_KEY, message)

  console.log("send", message)

  timeout = setTimeout(beMaster, 500)
}

const beSlave = async () => {
  if (role !== "slave") {
    console.log(`Slave ${process.pid} is getting messages`)

    role = "slave"
  }

  const message = await redis.rpop(MESSAGES_KEY)

  if (message) {
    console.log("get", message)

    if (Math.random() < 0.05) {
      await redis.rpush(ERRORS_KEY, message)

      console.log(`Error ${message} was found`)
    }
  }

  timeout = setTimeout(beSlave, 500)
}

const beMasterOrSlave = async () => {
  clearTimeout(timeout)

  return (await redis.set(MASTER_KEY, process.pid, "EX", 10, "NX")) === "OK"
    ? beMaster()
    : beSlave()
}

await redis.config("SET", "notify-keyspace-events", "KEA")

const subscriber = new Redis(redisUrl)

await subscriber.subscribe("__keyevent@0__:expired")

subscriber.on("message", async (_, message) => {
  try {
    if (message !== MASTER_KEY) {
      return
    }

    console.log("Master has been expired")

    await beMasterOrSlave()
  } catch (error) {
    console.log(error)
  }
})

await beMasterOrSlave()
