const utils = require('./utils')
const uploadS3 = require('./upload-s3')
const pgdump = require('./pgdump')
const decorateWithIamToken = require('./iam')
const decorateWithSecretsManagerCredentials = require('./secrets-manager')
const parseDatabaseNames = require('./parseDatabaseNames')
const encryption = require('./encryption')

const DEFAULT_CONFIG = require('./config')

async function backup(config) {
    if (!config.PGDATABASE) {
        throw new Error('PGDATABASE not provided in the event data')
    }
    if (!config.S3_BUCKET) {
        throw new Error('S3_BUCKET not provided in the event data')
    }

    const key = utils.generateBackupPath(
        config.PGDATABASE,
        config.ROOT
    )

    console.info('Backing up', config.PGDATABASE, 'to', key)
    // spawn the pg_dump process
    let stream = await pgdump(config)
    console.info('pg_dump process started')
    if (config.ENCRYPT_KEY && encryption.validateKey(config.ENCRYPT_KEY)) {
        // if encryption is enabled, we generate an IV and store it in a separate file
        const iv = encryption.generateIv()
        const ivKey = key + '.iv'

        await uploadS3(iv.toString('hex'), config, ivKey)
        stream = encryption.encrypt(stream, config.ENCRYPT_KEY, iv)
    }
    // stream the backup to S3
    return uploadS3(stream, config, key)
}

async function handler(event) {

    event.PGDATABASE = process.env.PGDATABASE || event.PGDATABASE
    event.PGUSER = process.env.PGUSER || event.PGUSER
    event.PGPASSWORD = process.env.PGPASSWORD || event.PGPASSWORD
    event.PGHOST = process.env.PGHOST || event.PGHOST
    event.S3_BUCKET = process.env.S3_BUCKET || event.S3_BUCKET
    event.ROOT = process.env.ROOT || event.ROOT
    event.USE_IAM_AUTH = process.env.USE_IAM_AUTH || event.USE_IAM_AUTH
    event.SECRETS_MANAGER_SECRET_ID = process.env.SECRETS_MANAGER_SECRET_ID || event.SECRETS_MANAGER_SECRET_ID

    console.log('PGDATABASE', event.PGDATABASE)
    console.log('PGUSER', event.PGUSER)
    console.log('PGPASSWORD is present', event.PGPASSWORD !== undefined)
    console.log('PGHOST', event.PGHOST)
    console.log('S3_BUCKET', event.S3_BUCKET)
    console.log('ROOT', event.ROOT)
    console.log('USE_IAM_AUTH', event.USE_IAM_AUTH)
    console.log('SECRETS_MANAGER_SECRET_ID', event.SECRETS_MANAGER_SECRET_ID)

    let results = []
    const baseConfig = { ...DEFAULT_CONFIG, ...event }
    let decoratedConfig

    if (event.USE_IAM_AUTH === true) {
        decoratedConfig = decorateWithIamToken(baseConfig)
    }
    else if (event.SECRETS_MANAGER_SECRET_ID) {
        decoratedConfig = await decorateWithSecretsManagerCredentials(baseConfig)
    }
    else {
        decoratedConfig = baseConfig
    }

    const dbnames = parseDatabaseNames(decoratedConfig)
    if (!dbnames || !dbnames.length) {
        throw new Error("PGDATABASE does not contain a database name")
    }

    // sequentially backup the configured database names (1 or more)
    for (const dbname of dbnames) {
        try {
            const dbconfig = {
                ...decoratedConfig,
                PGDATABASE: dbname
            }
            results.push(await backup(dbconfig))
        }
        catch (error) {
            // log the error and rethrow for Lambda
            if (process.env.NODE_ENV !== 'test') {
                console.error(error)
            }
            throw error
        }
    }

    return results.length > 1 ? results : results[0]
}

module.exports = handler
