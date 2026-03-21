export type RateLimitHeaderSource =
    | Headers
    | Response
    | { headers: Headers }
    | Record<string, string | string[] | undefined>;

const sleep = (ms: number) =>
    new Promise<void>((resolve) => setTimeout(resolve, ms));

function headerValue(
    source: RateLimitHeaderSource,
    name: string
): string | null {
    const lower = name.toLowerCase();
    if (source instanceof Headers) {
        return source.get(lower) ?? source.get(name);
    }
    if (typeof source === "object" && source !== null && "headers" in source) {
        const h = (source as { headers: Headers }).headers;
        if (h instanceof Headers) {
            return h.get(lower) ?? h.get(name);
        }
    }
    if (typeof source === "object" && source !== null) {
        const rec = source as Record<string, string | string[] | undefined>;
        for (const [k, v] of Object.entries(rec)) {
            if (k.toLowerCase() !== lower || v == null) continue;
            return Array.isArray(v) ? (v[0] ?? null) : v;
        }
    }
    return null;
}

export function parseRateLimitRemaining(source: RateLimitHeaderSource): number | null {
    const raw = headerValue(source, "x-ratelimit-remaining");
    if (raw == null || raw === "") return null;
    const n = Number.parseInt(raw.split(",")[0].trim(), 10);
    return Number.isFinite(n) ? n : null;
}

export function parseRateLimitResetSeconds(source: RateLimitHeaderSource): number | null {
    const raw = headerValue(source, "x-ratelimit-reset");
    if (raw == null || raw === "") return null;
    const n = Number.parseFloat(raw.split(",")[0].trim());
    return Number.isFinite(n) && n >= 0 ? n : null;
}

type RateSnapshot = {
    remaining: number;
    resetDeadlineMs: number;
};

let snapshot: RateSnapshot | null = null;

const RESET_BUFFER_MS = 250;

export function recordRateLimitFromResponse(source: RateLimitHeaderSource): void {
    const remaining = parseRateLimitRemaining(source);
    if (remaining === null) return;
    const resetSec = parseRateLimitResetSeconds(source);
    const resetDeadlineMs =
        resetSec !== null
            ? Date.now() + resetSec * 1000 + RESET_BUFFER_MS
            : Date.now() + 60_000;
    snapshot = { remaining, resetDeadlineMs };
}

export async function ensureMinRequestsAvailable(
  minAvailable = 4
): Promise<void> {
  if (!snapshot || snapshot.remaining >= minAvailable) return;

  const waitMs = Math.max(0, snapshot.resetDeadlineMs - Date.now());

  console.log(`[RateLimit] Waiting ${waitMs}ms`);
  await sleep(waitMs);

  // ✅ DO NOT NULL IT
  snapshot = {
    remaining: minAvailable,
    resetDeadlineMs: Date.now() + 1000
  };
}

export async function waitIfBelowMinFromHeaders(
    source: RateLimitHeaderSource,
    minAvailable = 4
): Promise<void> {
    const remaining = parseRateLimitRemaining(source);
    if (remaining === null || remaining >= minAvailable) return;
    const resetSec = parseRateLimitResetSeconds(source);
    const ms =
        resetSec !== null
            ? resetSec * 1000 + RESET_BUFFER_MS
            : 60_000 + RESET_BUFFER_MS;
    await sleep(ms);
}

export function withRobloxMinRateLimit<
    A extends unknown[],
    R extends Response,
>(fn: (...args: A) => Promise<R>, minAvailable = 4): (...args: A) => Promise<R> {
    return async (...args: A) => {
        await ensureMinRequestsAvailable(minAvailable);
        const res = await fn(...args);
        recordRateLimitFromResponse(res);
        await ensureMinRequestsAvailable(minAvailable);
        return res;
    };
}
