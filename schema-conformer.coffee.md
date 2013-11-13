Schema conformer
================

Takes a BigQuery schema and a stream of line delimited JSON, and outputs a
version of that stream which obeys the schema.  This is useful because BigQuery
totally chokes on any non-conforming data.

    stream = require 'stream'
    class Conformer extends stream.Transform
      @transforms = []

      constructor: (schema, options) ->
        return new Conformer if this is global
        @schema = @getSchema schema
        util = require 'util'
        @buffer = ''
        super options

The schema is an array of objects with the following fields:
  name   (required): name of the field
  type   (required): STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP or RECORD
  mode   (optional): NULLABLE, REQUIRED, REPEATED
  fields (optional): a nested schema for this field if the type is RECORD

But we store it as an object instead of an array so we can check it quickly.

      getSchema: (schema) ->
        return schema unless schema?
        o = {}
        o[name] = {name, type, mode, fields: @getSchema fields} for {name, type, mode, fields} in schema
        o

We conform by collecting data until we hit a newline.

      _transform: (chunk, encoding, callback) ->
        @buffer += chunk.toString 'utf8'
        lines = @buffer.split /\r?\n/
        @buffer = lines.pop()

        for line in lines

Parsing the data as JSON

          obj = JSON.parse line

Performing a series of transforms on each field

          perField @schema, obj, transform for transform in Conformer.transforms

transform @schema, obj

And then emitting the resulting JSON string

          @push JSON.stringify(obj)+'\n', 'utf8'

Transforms
----------

These are the transforms we use on each field:

    perField = (schema, obj, fn) ->
      for k, vs of obj
        vs = [vs] if schema[k]?.mode isnt 'REPEATED'
        for v in vs
          fn {type: schema[k], obj, k, v}
          if obj[k] and schema[k]?.type is 'RECORD'
            perField schema[k].fields, v, fn

1. Remove any fields not listed in the schema

    removeExtraFields = ({type, k, obj}) -> delete obj[k] unless type
    Conformer.transforms.push removeExtraFields

2. Coerce fields to a useful type if they're not already that type
    
    coerceInts = ({type, k, v, obj}) -> obj[k] = parseInt v if type.type is 'INTEGER'
    coerceFloats = ({type, k, v, obj}) -> obj[k] = parseFloat v if type.type is 'FLOAT'
    coerceBools = ({type, k, v, obj}) -> obj[k] = !!v if type.type is 'BOOLEAN' and v?

    Conformer.transforms.push coerceInts, coerceFloats, coerceBools

    module.exports = Conformer
