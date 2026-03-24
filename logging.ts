import fs from "fs/promises"

const errorFile = "./errors.txt"
const migrationFile = "./migrations.txt"
const deletionFile = "./deleted.txt"

// ✅ Only track completed users
export const userCache = new Set<string>()

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

// --------------------
// LOAD
// --------------------

export async function loadCache() {
    try {
        const raw = await fs.readFile(migrationFile, "utf8")
        const rows = raw.split(/\r?\n/)

        for (const row of rows) {
            if (!row) continue

            // ✅ handle both formats
            const userId = row.includes(";")
                ? row.split(";")[0]
                : row.trim()

            userCache.add(userId)
        }
    } catch {}
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

// --------------------
// WRITE
// --------------------

export function cacheUser(userId: string) {
    if (userCache.has(userId)) return

    userCache.add(userId)

    migrationBuffer.push(`${userId}\n`)
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

// --------------------
// CHECKS
// --------------------

export function isUserCached(userId: string) {
    return userCache.has(userId)
}

// --------------------
// FULL SAVE (fallback)
// --------------------

export async function saveCache() {
    await fs.writeFile(
        migrationFile,
        [...userCache].join("\n")
    )
}

export async function saveErrors() {
    await fs.writeFile(errorFile, errorCache.join("\n"))
}