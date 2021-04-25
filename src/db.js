const { Pool } = require('pg');
const config = require('./config');
const pool = new Pool(config.db);
/**
 * Query the database using the pool
 * @param {*} params
 */
async function log(params) {
    let query = 'INSERT INTO PUBLIC."first-task_batch-1" (worker, sequence, batch, client_time, details, task, type, server_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)'
    const client = await pool.connect()
    try {
        await client.query(query, params)
    } finally {
        client.release()
    }
}


module.exports = {
    log
}