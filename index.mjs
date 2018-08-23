#!/usr/bin/env node --experimental-modules
import yargs from "yargs"
import Redis from "ioredis"
import randomize from "randomatic"

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

class App {
  constructor({ redisClient, pid }) {
    this.redisClient = redisClient

    this.pid = pid
  }

  async chooseRole() {
    if (
      (await this.redisClient.set(MASTER_KEY, this.pid, "EX", 10, "NX")) ===
      "OK"
    ) {
      return this.beMaster()
    }

    return this.beSlave()
  }

  async beMaster() {
    const currentMaster = await this.redisClient.get(MASTER_KEY)

    if (currentMaster === null || Number(currentMaster) !== this.pid) {
      return
    }

    if (this.role !== "master") {
      console.log(`Master ${this.pid} is sending messages`)

      this.role = "master"
    }

    const message = randomize("*", 10)

    await this.redisClient.rpush(MESSAGES_KEY, message)

    console.log("send", message)

    setTimeout(() => this.beMaster(), 500)
  }

  async beSlave() {
    const currentMaster = await this.redisClient.get(MASTER_KEY)

    if (currentMaster === null || Number(currentMaster) === this.pid) {
      return
    }

    if (this.role !== "slave") {
      console.log(`Slave ${this.pid} is getting messages`)

      this.role = "slave"
    }

    const message = await this.redisClient.rpop(MESSAGES_KEY)

    if (message) {
      console.log("get", message)

      if (Math.random() < 0.05) {
        await this.redisClient.rpush(ERRORS_KEY, message)

        console.log(`Error ${message} was found`)
      }
    }

    setTimeout(() => this.beSlave(), 500)
  }
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

  const app = new App({ redisClient: redis, pid: process.pid })

  redis.on("ready", () => redis.config("SET", "notify-keyspace-events", "KEA"))

  const expirationListener = new Redis(redisUrl)

  expirationListener.subscribe("__keyevent@0__:expired")

  expirationListener.on("message", async (_, message) => {
    try {
      if (message !== MASTER_KEY) {
        return
      }

      console.log("Master has been expired")

      await app.chooseRole()
    } catch (err) {
      console.log(err)
    }
  })

  await app.chooseRole()
})().catch(console.log)
