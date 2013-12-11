module.exports = (schema) ->

  schema.defineTable 'User', [
    {col: 'uuid', type: 'uuid', unique: true}
    {col: 'login', type: 'string', unique: true}
    {col: 'email', type: 'email', unique: true}
  ]

  schema.defineTable 'password', [
    {col: 'type', type: 'string'}
    {col: 'salt', type: 'hexString', unique: true, default: {proc: 'randomBytes'}}
    {col: 'hash', type: 'hexString'}
    {col: 'userUUID', type: 'uuid', index: true, reference: {table: 'User', columns: ['uuid']}}
  ]

#  schema.defineIndex {
#    name: 'indexUserPassword'
#    index: ['userUUID']
#    table: 'Password'
#    reference:
#      table: 'User'
#      columns: ['uuid']
#  }

  schema
