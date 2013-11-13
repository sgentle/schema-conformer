Conformer = require '../schema-conformer'
fs = require 'fs'

unless schemaFile = process.argv[2]
  return console.warn "Usage: schema-conformer schema.json < data.json"

schema = JSON.parse fs.readFileSync(schemaFile, 'utf8')

conformer = new Conformer schema

process.stdin.pipe(conformer).pipe(process.stdout)

