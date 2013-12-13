module.exports = (schema) ->

  schema.defineTable 'User', [
    {col: 'uuid', type: 'uuid', default: {proc: 'makeUUID'}, unique: true}
    {col: 'login', type: 'string', unique: true}
    {col: 'email', type: 'email', unique: true}
  ]

  schema.defineTable 'Password', [
    {col: 'type', type: 'string', default: 'sha256'}
    {col: 'salt', type: 'hexString', unique: true, default: {proc: 'randomBytes'}}
    {col: 'hash', type: 'hexString'}
    {col: 'userUUID', type: 'uuid', index: true, reference: {table: 'User', columns: ['uuid']}}
  ], {
    verify: (passwd, cb) ->
      if passwd == 'mock-password'
        cb null, @
      else
        cb new Error("invalid_password")
  }

#  schema.defineIndex {
#    name: 'indexUserPassword'
#    index: ['userUUID']
#    table: 'Password'
#    reference:
#      table: 'User'
#      columns: ['uuid']
#  }

  schema
