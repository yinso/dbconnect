_ = require 'underscore'
uuid = require 'node-uuid'
async = require 'async'
{EventEmitter} = require 'events'
{check, sanitize} = require 'validator'
crypto = require 'crypto'

class Column
  constructor: (@table, @def) ->
    {col, type, optional} = @def
    @name = col
    schemaType = @table.schema.hasType type
    if not schemaType
      throw new Error("unknown_type: #{type}")
    @type = schemaType
    @optional = optional or false
    if @def.default
      if @def.default instanceof Object
        proc = @table.schema.hasFunction @def.default.proc
        if not proc
          throw new Error("unknown_default_function: #{@def.default.proc}")
        @default = proc
      else
        @default = (cb) => @def.default
      @optional = true
  destroy: () ->
    delete @table
    delete @type
    delete @default
  serialize: () ->
    @def
  validate: (val) ->
    if val != undefined and val != null
      @type.convertable val
    else if @optional
      true
    else
      throw new Error("value_required: #{@table.name}.#{@name}")
  make: (val) ->
    if val != undefined and val != null
      @type.make val
    else if @optional
      if @default
        @default()
      else
        null
    else
      throw new Error("value_required: #{@table.name}.#{@name}")

class Columns extends Array
  constructor: (columns) ->
    for col in columns
      @ensureColumn col
    for col in columns
      @push col
      @[col.name] = col
  destroy: () ->
    for key, val of @
      if val instanceof Index
        val.destroy()
  ensureColumn: (col) ->
    if not (col instanceof Column)
      throw new Error("columns.ctor:not_a_column: #{col}")
  splice: (index, removed, inserted...) ->
    for col in inserted
      @ensureColumn col
    removedCols = super index, removed, inserted...
    for col in removedCols
      delete @[col.name]
    for col in inserted
      @[col.name] = col
    removedCols
  push: (col...) ->
    @splice @length, 0, col...
  pop: (col...) ->
    res = @splice @length - 1, 1
    res[0]
  unshift: (col...) ->
    @splice 0, 0, col...
  shift: () ->
    res = @splice 0, 1
    res[0]

class Reference
  constructor: (@schema, @index, @table, @columns) ->

class Index
  constructor: (@table, args) ->
    # index/primary/unique can only have one of them.
    # default is index.
    # foreign key is setting up relations between objects - that's defined at schema level as well..
    {index, primary, unique, name, reference} = args
    # order. primary > unique > index
    if primary
      @init primary, name
      @ensurePrimary()
    else if unique
      @init unique, name
      @ensureUnique()
    else
      @init index, name
    if reference
      @ensureReference reference
  destroy: () ->
    delete @table
  init: (columns, name) ->
    for col in columns
      if not @table.hasColumn col
        throw new Error("unknown_column_in_table: #{col}, #{@table.name}")
    @columns = columns
    @name = (if not name then @makeName() else name)
    @table.schema.registerIndex @
  makeName: () ->
    columnName = @columns.join('_')
    "#{@table.name}_#{columnName}"
  ensurePrimary: () ->
    @table.setPrimary @
  ensureUnique: () ->
    @unique = true
  ensureReference: (@reference) ->
    {table, columns} = @reference
    # first - the table must exist.
    refTable = @table.schema.hasTable table
    if not refTable
      throw new Error("Unknown_reference_table: #{table}")
    for col in columns
      if not refTable.hasColumn col
        throw new Error("unknown_reference_column: #{table}.#{col}")
    @table.schema.registerReference @, refTable, columns
  serialize: () ->
    index = {table: @table.name, name: @name}
    if @reference
      index.reference = @reference
    if @primary
      index.primary = @columns
    else if @unique
      index.unique = @columns
    else
      index.index = @columns
    index

class Table
  constructor: (@schema, @name, @defs, @mixin) ->
    if @schema.hasTable @name
      throw new Error("duplicate_table_in_schema: #{@name}, #{@schema.name}")
    @initColumns()
    @initIndexes()
  destroy: () ->
    delete @schema
    for key, index of indexes
      index.destroy()
    delete @indexes
    @columns.destroy()
  ensureColumnNames: (columns) ->
    #console.log 'ensureColumNames', columns
    names = {}
    for col in columns
      if names.hasOwnProperty(col.col)
        throw new Error("duplicate_column_in_table: #{col.col}, #{@table.name}")
      else
        names[col.col] = col
  extractColumns: (defs) ->
    _.filter defs, (obj) -> obj.col or obj.column
  initColumns: () ->
    columns = @extractColumns @defs
    @ensureColumnNames columns
    @columns = new Columns(@makeColumn(col) for col in columns)
  makeColumn: (col) ->
    new Column @, col
  extractIndexes: (defs) ->
    helper = (def) ->
      def.index or def.primary or def.unique or def.reference
    _.filter defs, helper
  initIndexes: () ->
    @indexes = {}
    indexes = @extractIndexes @defs
    for def in indexes
      indexDef = @normalizeIndexDef def
      index = new Index @, indexDef
      @indexes[index.name] = index
  normalizeIndexDef: (def) ->
    if def.col
      col =
        if def.primary
          {primary: [def.col]}
        else if def.unique
          {unique: [def.col]}
        else
          {index: [def.col]}
      if def.reference
        col.reference = def.reference
      if def.name
        col.name = def.name
      col
    else
      def
  hasColumn: (col) ->
    #console.log 'Table.hasColumn', col, @columns
    if @columns.hasOwnProperty(col)
      @columns[col]
    else
      undefined
  hasPrimary: ()  ->
    @primary
  setPrimary: (index) ->
    if @primary
      throw new Error("Table_cannot_have_multiple_primary_keys")
    index.primary = true
    @primary = index
  hasUnique: () ->
    for key, index of @indexes
      if index.unique
        return index
    undefined
  serialize: () ->
    for col in @columns
      col.serialize()
  validate: (val) ->
    for col in @columns
      col.validate val[col.name]
  make: (val) ->
    #console.log "#{@name}.make", val
    obj = {}
    for col in @columns
      obj[col.name] = col.make val[col.name]
    type = @schema.hasType @name
    if type
      type.make obj
    else
      obj
  idQuery: (query) ->
    primary = @hasPrimary()
    if primary
      @_idQuery primary, query
    else
      unique = @hasUnique()
      if unique
        @_idQuery unique, query
      else
        query
  _idQuery: (index, query) ->
    obj = {}
    for col in index.columns
      obj[col] = query[col]
    obj


class ActiveRecord extends EventEmitter
  constructor: (@table, @db, @record) ->
    @changed = false
    @deleted = false
    @updated = {}
  set: (key, val) ->
    if @deleted
      throw new Error("ActiveRecord.set:record_already_deleted")
    if arguments.length == 2
      @_setOne key, val
    else if arguments[0] instanceof Object
      for k, v of key
        @_setOne k, v
    else
      throw new Error("ActiveRecord.set:invalid_args: #{key}, #{val}")
  _setOne: (key, val) ->
    col = @table.hasColumn key
    if col and not col.validate(val)
      throw new Error("#{table.name}.#{col.name}:fail_validation: #{val}")
    @updated[key] = val
    @changed = true
  get: (key) ->
    if @deleted
      throw new Error("ActiveRecord.get:record_already_deleted")
    if @updated.hasOwnProperty(key)
      @updated[key]
    else if @record.hasOwnProperty(key)
      @record[key]
    else
      undefined
  idQuery: () ->
    if @deleted
      throw new Error("ActiveRecord.idQuery:record_already_deleted")
    @table.idQuery @record
  update: (keyVals, cb) ->
    if @deleted
      return cb new Error("ActiveRecord.update:record_already_deleted")
    try
      @set keyVals
      @save cb
    catch e
      cb e
  save: (cb) ->
    if @deleted
      return cb new Error("ActiveRecord.save:record_already_deleted")
    if @changed
      query = @db.generateUpdate @table, @updated, @idQuery()
      @db.query query, {}, (err, res) =>
        if err
          cb err
        else
          _.extend @record, @updated
          @updated = {}
          @changed = false
          cb null, @
    else
      cb null, @
  delete: (cb) ->
    if @deleted
      return cb new Error("ActiveRecord.delete:record_already_deleted")
    query = @db.generateDelete @table, @idQuery()
    @db.query query, {}, (err, res) =>
      if err
        cb err
      else
        @deleted = true
        cb null

class Schema
  @builtInTypes: {}
  @builtInFunctions: {}
  @Record: ActiveRecord
  @registerType: (name, type) ->
    if @builtInTypes.hasOwnProperty(name)
      throw new Error("built_type_duplicate: #{name}")
    @builtInTypes[name] = type
  @registerFunction: (name, proc) ->
    if @builtInFunctions.hasOwnProperty(name)
      throw new Error("builtin_function_duplicate: #{name}")
    @builtInFunctions[name] = proc
  constructor: (schema) ->
    @types = {}
    @tables = {}
    @indexes = {}
    @references = {} # how are things related to another table...
    @functions = {}
    if schema
      @initialize schema
  destroy: () ->
    for key, index of @indexes
      index.destroy()
    delete @indexes
    for key, table of @tables
      table.destroy()
    delete @tables
  initialize: (schema) ->
    {@name, tables, indexes} = schema
    if tables
      for key, val of tables
        @defineTable key, val
    if indexes
      for def in indexes
        @defineIndex def
  defineTable: (name, defs, mixin = {}) ->
    table = new Table @, name, defs, mixin
    @tables[table.name] = table
  defineIndex: (def) ->
    if not def.table
      throw new Error("index_requires_table: #{def}")
    table = @hasTable def.table
    if not table
      throw new Error("index_table_unknown: #{def.table}")
    new Index table, def
  registerFunction: (name, proc) ->
    if @functions.hasOwnProperty(name)
      throw new Error("function_duplicated: #{name}")
    @functions[name] = proc
  hasFunction: (name) ->
    if @functions.hasOwnProperty(name)
      @functions[name]
    else if @constructor.builtInFunctions.hasOwnProperty(name)
      @constructor.builtInFunctions[name]
    else
      undefined
  registerType: (name, type) ->
    if @types.hasOwnProperty(name)
      throw new Error("duplicate_type: #{name}")
    @types[name] = type
  registerTableType: (name, type) ->
    if not type.hasOwnProperty('spec')
      throw new Error("lack_of_table_spec: #{name}")
    if not type.make # do I want active object?
      type.make = (obj) ->
        new type obj
    @defineTable name, type.spec
    @registerType name, type
  hasType: (name) ->
    if @types.hasOwnProperty(name)
      @types[name]
    else if @constructor.builtInTypes.hasOwnProperty(name)
      @constructor.builtInTypes[name]
    else
      undefined
  registerIndex: (index) ->
    if @indexes.hasOwnProperty index.name
      throw new Error("index_name_duplication: #{index.name}")
    @indexes[index.name] = index
  registerReference: (index, table, columns) ->
    @references[index.name] = new Reference @, index, table, columns
  hasTable: (name) ->
    if @tables.hasOwnProperty(name)
      @tables[name]
    else
      undefined
  makeRecord: (db, tableName, arg) ->
    table = @hasTable tableName
    if not table
      throw new Error("Schema.makeRecord:invalid_table: #{tableName}")
    rec = new ActiveRecord table, db, arg
    _.extend rec, table.mixin
  serialize: () ->
    tables = {}
    for key, table of @tables
      tables[key] = table.serialize()
    indexes = []
    for key, index of @indexes
      indexes.push index.serialize()
    {name: name, tables: tables, indexes: indexes}

class STRING
  @make: (val) ->
    if typeof(val) == 'string'
      val
    else if val instanceof Object
      JSON.stringify(val)
    else
      "#{val}"
  @convertable: (val) ->
    typeof(val) == 'string'

Schema.registerType 'string', STRING

class UUID
  @make: (val) ->
    if @convertable(val)
      val
    else
      uuid.v4()
  @convertable: (val) ->
    val.match /^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$/

Schema.registerType 'uuid', UUID

class NUMBER
  @convertable: (val) ->
    typeof(val) == 'number' or (typeof(val) == 'string' and val.match(/^-?\d*\.?\d*$/))
  @make: (val) ->
    if typeof(val) == 'number'
      val
    else
      parseInt(val)

Schema.registerType 'number', NUMBER

class EMAIL
  @convertable: (val) ->
    check(val).isEmail()
  @make: (val) ->
    if @convertable(val)
      val
    else
      throw new Error("invalid_email: #{val}")

Schema.registerType 'email', EMAIL

class HEXSTRING
  @convertable: (val) ->
    check(val).isHexadecimal(val)
  @make: (val) ->
    if @convertable(val)
      val
    else
      throw new Error("invalid_hexstring: #{val}")

Schema.registerType 'hexString', HEXSTRING

Schema.registerFunction 'randomBytes', (size = 32) ->
  b2h = []
  h2b = {}
  for i in [0...256] by 1
    b2h[i] = (i ^ 0x100).toString(16).substring(1)
    h2b[b2h[i]] = i
  toHex = (bytes) ->
    for byte in bytes
      b2h[byte]
  toHex(crypto.randomBytes(size)).join('')

Schema.registerFunction 'makeUUID', uuid.v4

module.exports = Schema
