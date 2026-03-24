export class SharedRateLimiter {
  private cooldownUntil = 0
  private nextAvailableAt = 0
  private readonly minIntervalMs: number

  // prevent log spam
  private lastLogTime = 0
  private readonly logCooldown = 1000 // 1s between logs

  constructor(requestsPerSecond: number) {
    this.minIntervalMs = Math.ceil(1000 / requestsPerSecond)
  }

  private log(message: string) {
    const now = Date.now()
    if (now - this.lastLogTime > this.logCooldown) {
      console.log(message)
      this.lastLogTime = now
    }
  }

  async waitTurn() {
    while (true) {
      const now = Date.now()

      // 🔴 HARD COOLDOWN (rate limit hit)
      if (now < this.cooldownUntil) {
        const wait = this.cooldownUntil - now
        this.log(`[RateLimit] HARD cooldown active → waiting ${wait}ms`)
        await sleep(wait)
        continue
      }

      // 🟡 SOFT LIMIT (RPS throttle)
      if (now < this.nextAvailableAt) {
        const wait = this.nextAvailableAt - now
        await sleep(wait)
        continue
      }

      this.nextAvailableAt = Date.now() + this.minIntervalMs
      return
    }
  }

  async updateFromHeaders(headers: Headers) {
    const remaining = Number(headers.get("x-ratelimit-remaining") ?? 0)
    const resetSeconds = Number(headers.get("x-ratelimit-reset") ?? 0)

    // 🔴 about to hit limit
    if (remaining <= 1) {
      this.log(`[RateLimit] ⚠️ NEAR LIMIT → remaining=${remaining}`)
    }

    // 🔴 cooldown trigger
    if (remaining < 6 && resetSeconds > 0) {
      const until = Date.now() + resetSeconds * 1000
      this.cooldownUntil = Math.max(this.cooldownUntil, until)

      console.log(
        `[RateLimit] HIT → remaining=${remaining}, cooldown=${resetSeconds}s`
      )
    }
  }
}

export function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms))
}

type RateLimiter = {
  waitTurn: () => Promise<void>
  updateFromHeaders: (headers: Headers) => Promise<void>
}

export function createRateLimitedFetch(limiter: RateLimiter): typeof fetch {
  return async (input, init) => {
    const maxRetries = 5

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await limiter.waitTurn()

        const res = await fetch(input, init)

        await limiter.updateFromHeaders(res.headers)

        return res
      } catch (err: any) {
        const isTimeout =
          err?.cause?.code === "UND_ERR_CONNECT_TIMEOUT" ||
          err?.code === "UND_ERR_CONNECT_TIMEOUT"

        if (!isTimeout || attempt === maxRetries) {
          throw err
        }

        const delay = Math.min(1000 * attempt, 5000)

        console.warn(
          `[Retry] attempt ${attempt}/${maxRetries} → waiting ${delay}ms`
        )

        await sleep(delay)
      }
    }

    throw new Error("unreachable")
  }
}