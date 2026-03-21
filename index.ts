import createClient from "openapi-fetch";
import type { paths } from "./OpenCloud.d.ts";
import chalk from "chalk";
import fs from "fs";
import {
    ensureMinRequestsAvailable,
    parseRateLimitRemaining,
    recordRateLimitFromResponse,
} from "./rate-limit.ts";

process.loadEnvFile("./.env")

const minRequestsNeeded = 6

const apiKey = process.env.API_KEY as string
const universeId = "9332753070" as string

const slotArray = Array.from({ length: 5 })
const cache = new Map<string, Set<number>>()

const cacheFile = "./migrations.txt"
const corruptedFile = "./corrupted.txt";

const client = createClient<paths>({
    headers: {
        ['x-api-key']: apiKey
    },
    baseUrl: "https://apis.roblox.com"
})

function getSlotSuffix(slot: number) {
    let suffix = ""
    switch (slot) {
        case 2:
            suffix = "-two"
            break;
        case 3:
            suffix = "-three"
            break;
        case 4:
            suffix = "-four"
            break;
        case 5:
            suffix = "-five"
            break;
        default:
            break;
    }

    return suffix
}

function logCorruptedKey(userId: string | number, slot: number, reason?: string) {
    const id = String(userId);
    const time = Date.now();

    const row = [
        id,
        slot,
        time,
        reason ?? ""
    ].join(";") + "\n";

    fs.appendFileSync(corruptedFile, row, "utf8");
}

function loadCache() {
    let raw = fs.readFileSync("./migrations.txt", "utf8")

    const rows = raw.split(/\r?\n/)

    for (const row of rows) {
        if (!row) continue

        const parts = row.trim().split(';')
        const userId = parts[0]

        const slots = new Set<number>()

        for (let i = 1; i < parts.length; i++) {
            const slot = Number(parts[i])
            if (!Number.isNaN(slot)) {
                slots.add(slot)
            }
        }

        cache.set(userId, slots)
    }
}

function persist() {
    const lines: string[] = []

    for (const [userId, slots] of cache) {
        const row = [userId, ...slots].join(';')
        lines.push(row)
    }

    fs.writeFileSync(cacheFile, lines.join('\n'), "utf8")
}

function addCache(userId: string | number, slot: number) {
    const id = String(userId)

    let slots = cache.get(id)
    if (!slots) {
        slots = new Set()
        cache.set(id, slots)
    }

    if (slots.has(slot)) return false // already exists

    slots.add(slot)

    return true
}

async function* iterateUsersToMigrate() {
    let cursor: string | undefined = undefined

    while (true) {
        await ensureMinRequestsAvailable(1)
        const res = await client.GET(
            "/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}/entries",
            {
                params: {
                    path: {
                        universe_id: universeId,
                        data_store_id: "OfficialDatastore"
                    },
                    query: {
                        maxPageSize: 100,
                        showDeleted: false,
                        cursor
                    }
                }
            }
        )
        if (res.response) recordRateLimitFromResponse(res.response)

        if (res.error) {
            throw res.error
        }

        const data = res.data

        for (const entry of data.dataStoreEntries) {
            yield entry.id.slice("global/Player_".length)
        }

        if (!data.nextPageToken) break

        cursor = data.nextPageToken
    }
}

async function getMostRecentSaves(userId: number | string, slot: number) {
    await ensureMinRequestsAvailable(1)
    const res = await client.GET("/cloud/v2/universes/{universe_id}/ordered-data-stores/{ordered_data_store_id}/scopes/{scope_id}/entries", {
        params: {
            path: {
                universe_id: universeId,
                ordered_data_store_id: `${userId}`,
                scope_id: `PlayerSaveTimes2${getSlotSuffix(slot)}`
            },
            query: {
                maxPageSize: 2,
                orderBy: "value desc",
                filter: "entry >= 2"
            }
        }
    })
    if (res.response) recordRateLimitFromResponse(res.response)

    if (res.error) {
        console.warn(res.error)
        return null
    }

    if (!res.data || !res.data?.orderedDataStoreEntries) {
        return null
    }

    const firstSaves = res.data.orderedDataStoreEntries
    return firstSaves[0].value
}

async function getSlotData(timestamp: number | string, userId: number | string, slot: number) {
    await ensureMinRequestsAvailable(1)
    const res = await client.GET("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}/scopes/{scope_id}/entries/{entry_id}", {
        params: {
            path: {
                universe_id: universeId,
                data_store_id: userId.toString(),
                entry_id: timestamp.toString(),
                scope_id: `PlayerData2${getSlotSuffix(slot)}`
            }
        }
    })
    if (res.response) recordRateLimitFromResponse(res.response)

    if (res.error) {
        return { errorCode: (res.error as any)?.code }
    }

    if (!res.data || !res.data?.value) {
        return { data: null }
    }

    return { data: res.data.value }
}

async function migrateIfNeeded(userId: string | number, slot: number) {
    const recentSave = await getMostRecentSaves(userId, slot)
    if (!recentSave) return "NO_DATA";

    const oldData = await getSlotData(recentSave, userId, slot)
    if (oldData && oldData?.errorCode == 9) return "INVALID_JSON";
    if (!oldData?.data) return "NO_DATA";

    await ensureMinRequestsAvailable(1)
    const res = await client.POST("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}/entries", {
        params: {
            path: {
                universe_id: universeId,
                data_store_id: `PlayerDataV2`,
            },
            query: {
                id: `${userId}/${slot}`
            },
        },
        body: {
            attributes: {
                ["Origin"]: "OpenCloud"
            } as any,
            value: JSON.stringify(oldData),
            users: [`users/${userId.toString()}`],
        }
    })
    if (res.response) recordRateLimitFromResponse(res.response)

    console.log(parseRateLimitRemaining(res.response))

    if (((res.error as any)?.code) == 3) {
        return "EXISTS"
    }

    return "MIGRATED"
}

async function deleteStore(userId: string | number) {
    await ensureMinRequestsAvailable(1)
    const res = await client.DELETE("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}", {
        params: {
            path: {
                universe_id: universeId,
                data_store_id: `${userId}`
            }
        }
    })

    if (res.error) {
        console.log(chalk.bgRed(`Unable to delete store <${userId}>:`, (res.error as any)?.message))
    }

    if (res.response.status == 200) {
        return true
    }

    return false
}

const STATUS_COLORS = {
    ["MIGRATED"]: chalk.green,
    ["EXISTS"]: chalk.yellow,
    ["NO_DATA"]: chalk.red
}

function safePersist(reason: string, err?: unknown) {
  try {
    console.log(chalk.red(`\n[CRASH] Saving cache due to: ${reason}`))
    if (err) console.error(err)

    persist()

    console.log(chalk.green("[CRASH] Cache saved successfully"))
  } catch (e) {
    console.error("[CRASH] Failed to save cache:", e)
  }
}

async function run() {
    loadCache()

    for await (const userId of iterateUsersToMigrate()) {
        let errored = false;
        let cached = cache.get(userId) || new Set()
        if (cached && cached.size == slotArray.length) continue;
        console.log(`Attempting migration for userId ${userId}`)
        for (const slotStr in slotArray) {
            const slot = parseInt(slotStr) + 1
            if (cached.has(slot)) continue;
            const status = await migrateIfNeeded(userId, slot)

            if (status == "INVALID_JSON") {
                logCorruptedKey(userId, slot, "INVALID")
                addCache(userId, slot)
                errored = true
                continue
            }

            if (status == "MIGRATED" || status == "EXISTS" || status == "NO_DATA") {
                addCache(userId, slot)
            }

            console.log(`[${userId}${getSlotSuffix(slot)}]: ${STATUS_COLORS[status](status)}`)
        }

        const deleted = await deleteStore(userId)
        if (deleted && !errored) {
            console.log(`${chalk.green("Migrated")} ${userId} ${chalk.green('successfully')}.`)
        }
    }

    persist()
}

process.on("SIGINT", () => {
  safePersist("SIGINT (Ctrl+C)")
  process.exit(0)
})

// kill signal
process.on("SIGTERM", () => {
  safePersist("SIGTERM")
  process.exit(0)
})

// uncaught sync errors
process.on("uncaughtException", (err) => {
  safePersist("uncaughtException", err)
  process.exit(1)
})

// unhandled async errors
process.on("unhandledRejection", (err) => {
  safePersist("unhandledRejection", err)
  process.exit(1)
})

run()