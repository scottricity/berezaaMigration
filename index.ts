import createClient from "openapi-fetch";
import type { paths } from "./OpenCloud.d.ts";
import chalk from "chalk";
import fs from "fs";
import {
    createRateLimitedFetch,
    SharedRateLimiter,
    sleep
} from "./rate-limit.ts";

import {
    cacheUser,
    isUserCached,
    userCache,
    loadCache,
    saveCache,
    loadErrors,
    logError,
    loadDeleted,
    deleted,
    cacheDelete,
    saveErrors,
    flushNow
} from "./logging.ts"

process.loadEnvFile("./.env")

const apiKey = process.env.API_KEY as string
const universeId = "105689481" as string

const ignoredStores = [
    "DataRollbackVersionV10",
    "DataRollbackVersionV8",
    "OfficialDatastore",
    "PlayerDataV2",
    "____PS",
    "ModerationDataVersion1",
    "__global__3a0c3317-5845-4d63-bd11-1acc26b8a6c3-1",
    "PlayerData",
    "PlayerInformation1.17",
    "PlayerStats",
    "Places",
    "PlayB",
    "PerkRecorder3"
]

const slotArray = ["1", "2", "3", "4", "5"]

const readLimiter = new SharedRateLimiter(166);   // ~10k/min
const writeLimiter = new SharedRateLimiter(33);   // ~2k/min
const deleteLimiter = new SharedRateLimiter(166); // ~10k/min

const readClient = createClient<paths>({
    headers: { ['x-api-key']: apiKey },
    baseUrl: "https://apis.roblox.com",
    fetch: createRateLimitedFetch(readLimiter)
});

const writeClient = createClient<paths>({
    headers: { ['x-api-key']: apiKey },
    baseUrl: "https://apis.roblox.com",
    fetch: createRateLimitedFetch(writeLimiter)
});

const deleteClient = createClient<paths>({
    headers: { ['x-api-key']: apiKey },
    baseUrl: "https://apis.roblox.com",
    fetch: createRateLimitedFetch(deleteLimiter)
});

function toSuffix(slot: number) {
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

async function* iterateUserIdStores(filter?: string) {
    let cursor: string | undefined = undefined

    while (true) {
        const res = await readClient.GET(
            "/cloud/v2/universes/{universe_id}/data-stores",
            {
                params: {
                    path: {
                        universe_id: universeId,
                    },
                    query: {
                        maxPageSize: 100,
                        showDeleted: false,
                        pageToken: cursor,
                        filter: filter || undefined
                    }
                }
            }
        )

        if (res.error) {
            console.warn(res.error)
            throw res.error
        }

        const data = res.data
        if (!data || !data.dataStores) continue;

        for (const entry of data.dataStores) {
            if (deleted.has(entry.id)) continue;
            const id = parseInt(entry.id)

            if (ignoredStores.includes(entry.id)) continue
            if (Number.isNaN(id)) continue
            if (id < -1) yield entry.id
            continue
        }

        if (!data.nextPageToken) break

        cursor = data.nextPageToken
    }
}

async function* iterateUsersToMigrate() {
    let cursor: string | undefined = undefined

    while (true) {
        const res = await readClient.GET(
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
                        pageToken: cursor
                    }
                }
            }
        )

        if (res.error) {
            throw res.error
        }

        const data = res.data

        for (const entry of data.dataStoreEntries) {
            let userId = entry.id.slice("global/Player_".length)
            let int = parseInt(userId)
            if (Number.isNaN(int)) continue;

            yield userId
        }

        if (!data.nextPageToken) break

        cursor = data.nextPageToken
    }
}

async function getMostRecentSaves(userId: number | string, slot: number) {
    const res = await readClient.GET("/cloud/v2/universes/{universe_id}/ordered-data-stores/{ordered_data_store_id}/scopes/{scope_id}/entries", {
        params: {
            path: {
                universe_id: universeId,
                ordered_data_store_id: `${userId}`,
                scope_id: `PlayerSaveTimes2${toSuffix(slot)}`
            },
            query: {
                maxPageSize: 2,
                orderBy: "value desc",
                filter: "entry >= 2"
            }
        }
    })

    if (res.error) {
        return null
    }

    if (!res.data || !res.data?.orderedDataStoreEntries) {
        return null
    }

    const firstSaves = res.data.orderedDataStoreEntries
    return firstSaves[0].value
}

async function getSlotData(timestamp: number | string, userId: number | string, slot: number) {
    const res = await readClient.GET("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}/scopes/{scope_id}/entries/{entry_id}", {
        params: {
            path: {
                universe_id: universeId,
                data_store_id: userId.toString(),
                entry_id: timestamp.toString(),
                scope_id: `PlayerData2${toSuffix(slot)}`
            }
        }
    })

    if (res.error) {
        let code = (res.error as any)?.code
        return { errorCode: code }
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
    if (oldData && oldData?.errorCode == 9) return "INVALID";
    if (!oldData?.data) return "NO_DATA";

    if (slot == 0) {
        slot = 1
    }

    const res = await writeClient.POST("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}/entries", {
        params: {
            path: {
                universe_id: universeId,
                data_store_id: `PlayerDataV2`
            },
            query: {
                id: `${userId}/${slot}`
            },
        },
        body: {
            attributes: {
                ["Origin"]: "OpenCloud",
                ["LastSave"]: recentSave
            } as any,
            value: oldData.data,
            users: [`users/${userId.toString()}`],
        }
    })


    if (((res.error as any)?.code) == 3) {
        return "EXISTS"
    }

    return "MIGRATED"
}

async function deleteStore(userId: string | number) {
    const res = await deleteClient.DELETE("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}", {
        params: {
            path: {
                universe_id: universeId,
                data_store_id: `${userId}`
            }
        }
    })

    if (res.error) {
        return false
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
const deleteQueue: (string | number)[] = []

const DELETE_CONCURRENCY = 10

let activeDeletes = 0
let resolveDrain: (() => void) | null = null

async function handleDelete(userId: string) {
    try {
        const didDelete = await deleteStore(userId)

        if (didDelete) {
            cacheDelete(userId)
            console.log(`🗑️ Deleted ${userId}`)
        } else {
            console.log(`⚠️ Failed delete ${userId}`)
        }
    } catch (err) {
        console.warn(`Delete error ${userId}`, err)
    }
}

function enqueueDelete(userId: string | number) {
    deleteQueue.push(userId)
    processDeleteQueue()
}

function processDeleteQueue() {
    while (activeDeletes < DELETE_CONCURRENCY && deleteQueue.length > 0) {
        const userId = deleteQueue.shift()
        if (!userId) continue

        activeDeletes++

        handleDelete(userId.toString())
            .catch(console.error)
            .finally(() => {
                activeDeletes--

                // continue processing immediately
                processDeleteQueue()

                // resolve drain if finished
                if (activeDeletes === 0 && deleteQueue.length === 0 && resolveDrain) {
                    resolveDrain()
                    resolveDrain = null
                }
            })
    }
}

async function drainDeletes() {
    if (activeDeletes === 0 && deleteQueue.length === 0) return

    return new Promise<void>((resolve) => {
        resolveDrain = resolve
    })
}

let migratedCount = 0

const WEBHOOK_URL = process.env.DISCORD_WEBHOOK!

async function sendWebhookEmbed(data: {
    title?: string
    description?: string
    color?: number
    fields?: { name: string; value: string; inline?: boolean }[]
}) {
    try {
        await fetch(WEBHOOK_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                embeds: [
                    {
                        title: data.title,
                        description: data.description,
                        color: data.color ?? 0x00ff00,
                        fields: data.fields ?? [],
                        timestamp: new Date().toISOString()
                    }
                ]
            })
        })
    } catch (err) {
        console.warn("Webhook failed:", err)
    }
}

let startTime = Date.now()
async function onMilestone(count: number) {
    console.log(`🎉 Milestone reached: ${count} users migrated`)
    const elapsed = (Date.now() - startTime) / 1000
    const rate = count / elapsed

    await sendWebhookEmbed({
        title: "🚀 Migration Milestone",
        description: `Reached **${count.toLocaleString()} users**`,
        color: 0x00ff00,
        fields: [
            {
                name: "Speed",
                value: `${rate.toFixed(2)} users/sec`,
                inline: true
            },
            {
                name: "Elapsed",
                value: `${elapsed.toFixed(1)}s`,
                inline: true
            },
        ]
    })
}

function incrementAndCheckMilestone() {
    migratedCount++

    if (migratedCount % 50_000 === 0) {
        onMilestone(migratedCount)
    }
}

async function processUser(userId: string) {
    if (isUserCached(userId)) return

    const start = Date.now()

    const results = await Promise.all(
        slotArray.map(async (_, i) => {
            const slot = i + 1

            try {
                const status = await migrateIfNeeded(userId, slot)

                if (status === "INVALID") {
                    logError(`${userId}-${slot}`, status)
                    return "INVALID"
                }

                return status
            } catch (error) {
                console.warn(`Error migrating ${userId}-${slot}`, error)
                return "ERROR"
            }
        })
    )

    const errored = results.includes("INVALID") || results.includes("ERROR")
    const totalTime = Date.now() - start

    if (!errored) {
        cacheUser(userId)
        enqueueDelete(userId)
        incrementAndCheckMilestone()
        console.log(`✅ MIGRATED ${userId} (${totalTime}ms)`)
    } else {
        console.log(`⚠️ PARTIAL ${userId} (${totalTime}ms)`)
    }
}

const concurrency = 5

async function run() {
    await loadCache()
    await loadDeleted()
    await loadErrors()

    migratedCount = userCache.size

    const iterator = iterateUsersToMigrate()

    const workers = Array.from({ length: concurrency }, async () => {
        for await (const userId of iterator) {
            if (isUserCached(userId)) continue

            try {
                await processUser(userId)
            } catch (err) {
                console.error(`Worker error for ${userId}`, err)
            }
        }
    })

    await Promise.all(workers)

    console.log("Draining deletes...")
    await drainDeletes()
}

process.on("SIGINT", async () => {
    console.log("Draining deletes...")
    await drainDeletes()

    await flushNow()
    await saveCache()
    await saveErrors()
    process.exit(0)
})

// kill signal
process.on("SIGTERM", async () => {
    await flushNow()
    await saveCache()
    await saveErrors()
    process.exit(0)
})

// uncaught sync errors
process.on("uncaughtException", async (err) => {
    await flushNow()
    await saveCache()
    await saveErrors()
    process.exit(1)
})

// unhandled async errors
process.on("unhandledRejection", async (err) => {
    await flushNow()
    await saveCache()
    await saveErrors()
    process.exit(1)
})

run()