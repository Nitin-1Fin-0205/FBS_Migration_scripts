const { Pool } = require('pg');
require('dotenv').config();

// Source database pool (AWS RDS)
const sourcePool = new Pool({
    user: process.env.DB_USER || '',
    password: process.env.DB_PASSWORD,
    host: process.env.DB_HOST || '',
    port: process.env.DB_PORT || 5432,
    database: process.env.DB_NAME || '',
});

const LackmasterDB = new Pool({
    user: process.env.DB_USER || '',
    password: process.env.DB_PASSWORD,
    host: process.env.DB_HOST || '',
    port: process.env.DB_PORT || 5432,
    database: process.env.LAKEMASTER_DB || '',
});

// Destination database pool (Local)
const destPool = new Pool({
    user: process.env.DB_USER1 || '',
    password: process.env.DB_PASSWORD1,
    host: process.env.DB_HOST1 || '',
    port: process.env.DB_PORT1 || 5432,
    database: process.env.DB_NAME1 || '',
});

// Set schema for destination pool using async initialization
destPool.on('connect', async (client) => {
    try {
        const schema = process.env.DB_SCHEMA1 || 'public';
        await client.query(`SET search_path TO ${schema}`);
    } catch (err) {
        console.error('Error setting schema:', err.message);
    }
});

module.exports = {
    sourcePool,
    destPool,
    LackmasterDB
};