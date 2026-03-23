export class SharedRateLimiter {
  private cooldownUntil = 0
  private nextAvailableAt = 0
  private readonly minIntervalMs: number

  constructor(requestsPerSecond: number) {
    this.minIntervalMs = Math.ceil(1000 / requestsPerSecond)
  }

  async waitTurn() {
    while (true) {
      const now = Date.now()

      if (now < this.cooldownUntil) {
        await sleep(this.cooldownUntil - now)
        continue
      }

      if (now < this.nextAvailableAt) {
        await sleep(this.nextAvailableAt - now)
        continue
      }

      this.nextAvailableAt = Date.now() + this.minIntervalMs
      return
    }
  }

  async updateFromHeaders(headers: Headers) {
    const remaining = Number(headers.get("x-ratelimit-remaining") ?? 0)
    const resetSeconds = Number(headers.get("x-ratelimit-reset") ?? 0)

    if (remaining < 6 && resetSeconds > 0) {
      const until = Date.now() + resetSeconds * 1000
      this.cooldownUntil = Math.max(this.cooldownUntil, until)

      console.log(
        `[RateLimit] remaining=${remaining}, cooldown ${resetSeconds}s`
      )
    }
  }
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms))
}

type RateLimiter = {
  waitTurn: () => Promise<void>
  updateFromHeaders: (headers: Headers) => Promise<void>
}

export function createRateLimitedFetch(limiter: RateLimiter): typeof fetch {
  return async (input, init) => {
    await limiter.waitTurn()

    const res = await fetch(input, init)

    await limiter.updateFromHeaders(res.headers)

    return res
  }
}