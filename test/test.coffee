Conformer = require '../schema-conformer'
assert = require 'assert'

test = ({schema, input, output}) ->
  conformer = new Conformer schema

  conformer.write JSON.stringify(input)+'\n', 'utf8'

  chunk = conformer.read()
  result = chunk?.toString('utf8')
  result = JSON.parse(result) if result
  assert.deepEqual output, result

describe 'conformer', ->
  describe 'remove fields transform', ->
    it 'should remove top-level fields not defined in the schema', ->
      test(
        schema: [{name: 'foo', type: 'INTEGER'}]
        input: {foo: 1, a: 'b'}
        output: {foo: 1}
      )

    it 'should remove nested fields not defined in the schema', ->
      test(
        schema: [
          {name: 'foo', type: 'INTEGER'}
          {name: 'bar', type: 'RECORD', fields: [
            {name: 'troz', type: 'INTEGER'}
          ]}
        ]
        input: {foo: 1, bar: {troz: 1, a: 'b'}}
        output: {foo: 1, bar: {troz: 1}}
      )

  describe 'coerce types transforms', ->
    it 'should coerce strings to ints', ->
      test(
        schema: [{name: 'foo', type: 'INTEGER'}]
        input: {foo: "1"}
        output: {foo: 1}
      )

    it 'should coerce strings to floats', ->
      test(
        schema: [{name: 'foo', type: 'FLOATS'}]
        input: {foo: "1.5"}
        output: {foo: 1.5}
      )

    it 'should coerce strings to booleans', ->
      test(
        schema: [{name: 'foo', type: 'BOOLEAN'}]
        input: {foo: "what up"}
        output: {foo: true}
      )

    it 'should coerce strings to booleans', ->
      test(
        schema: [{name: 'foo', type: 'BOOLEAN'}]
        input: {foo: ""}
        output: {foo: false}
      )

