const { Pool } = require('pg');
const config = require('./config');
const pool = new Pool(config.db);
/**
 * Query the database using the pool
 * @param {*} params
 * @param task
 * @param batch
 */
async function log(params, task, batch) {
    let tableName = '"' + task + "_" + batch + '"'
    let query = 'INSERT INTO PUBLIC.' + tableName + ' (worker, sequence, batch, client_time, details, task, type, server_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)'
    await pool.query(query, params)
}


module.exports = {
    log
}