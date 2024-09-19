const path = require('path');

// default config that is overridden by the Lambda event
module.exports = {
    S3_REGION: 'eu-west-1',
    PGDUMP_PATH: process.env.PGDUMP_PATH || path.join(__dirname, '../bin/postgres-14.5'),
    // maximum time allowed to connect to postgres before a timeout occurs
    PGCONNECT_TIMEOUT: 15,
    USE_IAM_AUTH: false,
    S3_STORAGE_CLASS: 'STANDARD',
    S3_PART_SIZE: 1073741824 // increase part size to 1GB
}
