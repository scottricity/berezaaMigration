import fs from "fs/promises"

const errorFile = "./errors.txt"
const migrationFile = "./migrations.txt"
const deletionFile = "./deleted.txt"

// userId -> Set<slot>
export const cache = new Map<string, Set<number>>()
let errorCache: string[] = []
export const deleted = new Set<string | number>()

let migrationBuffer: string[] = []
let errorBuffer: string[] = []
let deletionBuffer: string[] = []

let flushing = false

async function flush() {
    if (flushing) return
    flushing = true

    try {
        const jobs: Promise<any>[] = []

        if (migrationBuffer.length > 0) {
            const chunk = migrationBuffer.join("")
            migrationBuffer = []
            jobs.push(fs.appendFile(migrationFile, chunk))
        }

        if (errorBuffer.length > 0) {
            const chunk = errorBuffer.join("")
            errorBuffer = []
            jobs.push(fs.appendFile(errorFile, chunk))
        }

        if (deletionBuffer.length > 0) {
            const chunk = deletionBuffer.join("")
            deletionBuffer = []
            jobs.push(fs.appendFile(deletionFile, chunk))
        }

        if (jobs.length > 0) {
            await Promise.all(jobs)
        }
    } catch (err) {
        console.error("Flush error:", err)
    }

    flushing = false
}

setInterval(flush, 1000)

export async function flushNow() {
    await flush()
}

export async function loadErrors() {
    try {
        const raw = await fs.readFile(errorFile, "utf8")
        errorCache = raw.split(/\r?\n/).filter(Boolean)
    } catch {}
}

export async function loadDeleted() {
    try {
        const raw = await fs.readFile(deletionFile, "utf8")
        const lines = raw.split(/\r?\n/)

        for (const id of lines) {
            if (id) deleted.add(id)
        }
    } catch {}
}

export async function loadCache() {
    try {
        const raw = await fs.readFile(migrationFile, "utf8")
        const rows = raw.split(/\r?\n/)

        for (const row of rows) {
            if (!row) continue

            const parts = row.split(";")
            const userId = parts[0]

            let slots = cache.get(userId)
            if (!slots) {
                slots = new Set()
                cache.set(userId, slots)
            }

            for (let i = 1; i < parts.length; i++) {
                const slot = Number(parts[i])
                if (!Number.isNaN(slot)) {
                    slots.add(slot)
                }
            }
        }
    } catch {}
}

export function addSlot(userId: string, slot: number) {
    let slots = cache.get(userId)

    if (!slots) {
        slots = new Set()
        cache.set(userId, slots)
    }

    if (slots.has(slot)) return

    slots.add(slot)

    migrationBuffer.push(`${userId};${slot}\n`)
}

export function logError(key: string, reason: string) {
    const line = `${key};${reason}`
    errorCache.push(line)
    errorBuffer.push(line + "\n")
}

export function cacheDelete(key: string) {
    deleted.add(key)
    deletionBuffer.push(`${key}\n`)
}

export function isCached(userId: string, slot: number) {
    const slots = cache.get(userId)
    return slots ? slots.has(slot) : false
}

export function isUserComplete(userId: string) {
    const slots = cache.get(userId)
    return slots ? slots.size === 5 : false
}

export async function saveCache() {
    const lines: string[] = []

    for (const [userId, slots] of cache) {
        lines.push([userId, ...slots].join(";"))
    }

    await fs.writeFile(migrationFile, lines.join("\n"))
}

export async function saveErrors() {
    await fs.writeFile(errorFile, errorCache.join("\n"))
}