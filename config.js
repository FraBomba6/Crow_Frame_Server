const env = process.env;

const config = {
    db: {
        host: env.DB_HOST || 'localhost',
        port: env.DB_PORT || '5432',
        user: env.DB_USER || 'postgres',
        password: env.DB_PASSWORD || 'pass',
        database: env.DB_NAME || 'logDB',
    }
};

module.exports = config;