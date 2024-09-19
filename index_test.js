const handler = require('./lib/handler')

handler({
    "PGDATABASE": "mudflap",
    "PGUSER": "mudflap",
    "PGPASSWORD": "2P9b6WQkBeEvtEdYmzWEwgvWEJirjFbmxGZ93k7QuPX",
    "PGHOST": "mudflap-staging-restored-read-replica.cjzp98cumkfo.us-east-2.rds.amazonaws.com",
    "S3_BUCKET": "rds-backup-mudflap-staging",
    "ROOT": "pgdump-aws-lambda"
  })

// module.exports.handler = handler
