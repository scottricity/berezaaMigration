import createClient from "openapi-fetch";
import type { paths } from "./OpenCloud.d.ts";
import chalk from "chalk";
import fs from "fs";
import {
    createRateLimitedFetch,
    SharedRateLimiter
} from "./rate-limit.ts";

import {
    addSlot,
    loadCache,
    saveCache,
    isCached,
    isUserComplete,
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

const limiter = new SharedRateLimiter(5);
const client = createClient<paths>({
    headers: {
        ['x-api-key']: apiKey
    },
    baseUrl: "https://apis.roblox.com",
    fetch: createRateLimitedFetch(limiter)
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
        const res = await client.GET(
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
    const res = await client.GET("/cloud/v2/universes/{universe_id}/ordered-data-stores/{ordered_data_store_id}/scopes/{scope_id}/entries", {
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
    const res = await client.GET("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}/scopes/{scope_id}/entries/{entry_id}", {
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

    const res = await client.POST("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}/entries", {
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
    const res = await client.DELETE("/cloud/v2/universes/{universe_id}/data-stores/{data_store_id}", {
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

async function processUser(userId: string) {
    if (isUserComplete(userId)) return

    console.log(`MIGRATING ${userId}`)

    let errored = false

    await Promise.all(
        slotArray.map(async (_, i) => {
            const slot = i + 1
            if (isCached(userId, slot)) return

            addSlot(userId, slot)

            const status = await migrateIfNeeded(userId, slot)

            if (status === "INVALID") {
                logError(`${userId}-${slot}`, status)
                errored = true
                return
            }
        })
    )

    if (!errored) {
        await deleteStore(userId)
    }
}

const concurrency = 5
const workers: Promise<void>[] = []

async function run() {
    await loadCache()
    await loadDeleted()
    await loadErrors()

    for await (const userId of iterateUsersToMigrate()) {
        const p = processUser(userId)
        workers.push(p)

        if (workers.length >= concurrency) {
            await Promise.race(workers)
            workers.splice(workers.findIndex(w => w === p), 1)
        }
    }

    await Promise.all(workers)
}

process.on("SIGINT", async () => {
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