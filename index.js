#!/usr/bin/env node

import csv from "csv-parser"
import { parse } from "date-fns"
import { createReadStream } from "fs"
import path from "path"
import pkg from "pg"
import { finished } from "stream/promises"

const clientConfig = {
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "postgres",
    database: "players_db",
}

const dirname = path.dirname(new URL(import.meta.url).pathname)

const main = async () => {
    const argPath = process.argv[2]
    console.log(argPath)
    const client = new pkg.Client(clientConfig)
    await client.connect()

    await client.query(`
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            nickname VARCHAR(255),
            email VARCHAR(255) UNIQUE,
            registered INTEGER,
            status VARCHAR(10)
        );
    `)

    const filePath = path.join(dirname, argPath ?? "/data/file.csv")

    const promises = [];

    const rs = createReadStream(filePath);
    rs.pipe(csv({
        mapHeaders: ({ header }) => header.trim(),
        separator: "; ",
    }))
        .on("data", async (row) => {
            const parsedDate = parse(row['Зарегистрирован'].trim(), 'dd.MM.yyyy HH:mm', new Date())
            const registered = Math.floor(parsedDate.getTime() / 1000)
            const values = [
                row['Ник'].trim(),
                row['Email'].trim(),
                registered,
                row['Статус'].trim(),
            ]
            const promise = client.query("SELECT 1 FROM players WHERE email = $1", [values[1]])
                .then(async (res) => {
                    if (res.rows.length === 0) {
                        await client.query(
                            "INSERT INTO players (nickname, email, registered, status) VALUES ($1, $2, $3, $4)",
                            values
                        )
                        console.log(`Inserted row: ${JSON.stringify(values)}`)
                    } else {
                        console.log(`Email ${values[1]} already exists, skipping insert.`)
                    }
                })
                .catch((error) => {
                    console.error("Error inserting row:", error);
                })

            promises.push(promise)
        })

    await finished(rs)
    await Promise.all(promises)

    const res = await client.query('SELECT * FROM players WHERE status = $1 ORDER BY registered', ['On'])
    console.log(JSON.stringify(res.rows, null, 2))

    await client.end()
}

await main().catch((error) => {
    console.error('Error:', error)
    process.exit(1)
})