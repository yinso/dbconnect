
postgres = require 'pg'
DBConnect = require './dbconnect'
_ = require 'underscore'
Schema = require './schema'

class PostgresDriver extends DBConnect
  @defaultOptions:
    host: 'localhost'
    port: 5432
    database: 'postgres'
  connString: () ->
    # take the args and create a connection string from it.
    # format of connection string.
    # postgres://user:password@host:port/database
    {user, password, host, port, database} = @args
    if user and password
      "postgres://#{user}:#{password}@#{host}:#{port}/#{database}"
    else
      "postgres://#{host}:#{port}/#{database}"
  tableName: (name) ->
    # TableName
    # ==>
    # table_name_t
    splitted = name.split /([A-Z]+)/ # allow for consecutive capital chars to stay together.
    i = 0
    normalized =
      for str in splitted
        if str.match /[A-Z]+/
          if i++ == 0
            str.toLowerCase()
          else
            "_" + str.toLowerCase()
        else
          str
    #console.log 'PostgresDRiver.tableName', name, splitted, normalized
    normalized.join('') + "_t"
  connect: (cb) ->
    postgres.connect @connString(), (err, client, done) =>
      if err
        cb err
      else
        @inner = client
        @done = done
        cb null, @
  _query: (stmt, args, cb) ->
    if arguments.length == 2
      cb = args
      args = {}
    # we'll need to parse the query to convert $key to $n
    parsed = @parseStmt stmt, args
    console.log 'Postgres._query', parsed.stmt, parsed.args
    @inner.query parsed.stmt, parsed.args, (err, res) =>
      #console.log 'inner.query', parsed.stmt, parsed.args, err, res
      if err
        #console.log 'inner.query.hasError', err
        cb err
      else if stmt.selectOne
        cb null, res.rows[0]
      else if stmt.next
        @_query stmt.next, {}, cb
      else
        cb null, res.rows
  parseStmt: (stmt, args) ->
    if stmt instanceof Object and stmt.stmt and stmt.args
      {stmt, args} = stmt
    splitted = stmt.split /(\$[\w]+)/
    i = 1
    normalized = []
    normedArgs = []
    for s in splitted
      matched = s.match /^\$([\w]+)$/
      if matched
        #console.log 'Postgresql.parseStmt', s, matched[1], args[matched[1]], args.hasOwnProperty(matched[1]), matched
        if not args.hasOwnProperty(matched[1])
          throw new Error("Postgresql.query:stmt_missing_key: #{s}")
        normedArgs.push args[matched[1]]
        normalized.push "$#{i++}"
      else
        normalized.push s
    {stmt: normalized.join(''), args: normedArgs}
  disconnect: (cb) ->
    try
      @done()
      cb null
    catch e
      cb e
  beginTrans: (cb) ->
    @_query 'begin', {}, cb
  commit: (cb) ->
    @_query 'commit', {}, cb
  rollback: (cb) ->
    @_query 'rollback', {}, cb
  ensureColumns: (table, kv) ->
    for key, val of kv
      if not table.hasColumn key
        throw new Error("Postgresql.insert.unknown_column: #{key}")
  generateInsert: (table, args) ->
    if args instanceof Array
      throw new Error("Postgresql.insert:multi_insert_not_yet_supported: #{args}")
    else
      args = table.make args
      @ensureColumns table, args
    keys = []
    for key, val of args
      keys.push key
    columnText = keys.join(', ')
    phText = ("$#{key}" for key in keys).join(', ')
    # we'll also need a select statement for this guy... for this we should figure out the appropriate
    # unique
    idQuery = table.idQuery args
    select = @generateSelectOne table, idQuery
    {stmt: "insert into #{@tableName(table.name)} (#{columnText}) select #{phText}", args: args, next: select}
  escapeVal: (val) ->
    strHelper = (val) ->
      "'" + val.replace(/\'/g, "''") + "'"
    if typeof(val) == 'number'
      "#{val}"
    else if typeof(val) == 'string'
      strHelper val
    else
      strHelper val.toString()
  criteriaQuery: (query, sep = ' and ') ->
    criteria = []
    for key, val of query
      if val instanceof Array
        criteria.push "#{key} in (#{(@escapeVal(v) for v in val).join(', ')})"
      else
        criteria.push "#{key} = $#{key}"
    criteria.join(sep)
  generateDelete: (table, query) ->
    if Object.keys(query).length == 0
      {stmt: "delete from #{@tableName(table.name)}", args: query}
    else
      @ensureColumns table, query
      stmt = @criteriaQuery query
      {stmt: "delete from #{@tableName(table.name)} where #{stmt}", args: query}
  generateSelect: (table, query) ->
    if Object.keys(query).length == 0
      {stmt: "select * from #{@tableName(table.name)}", args: query}
    else
      @ensureColumns table, query
      stmt = @criteriaQuery query
      {stmt: "select * from #{@tableName(table.name)} where #{stmt}", args: query}
  generateSelectOne: (table, query) ->
    if Object.keys(query).length == 0
      {stmt: "select * from #{@tableName(table.name)}", args: query, selectOne: true}
    else
      @ensureColumns table, query
      stmt = @criteriaQuery query
      {stmt: "select * from #{@tableName(table.name)} where #{stmt}", args: query, selectOne: true}
  generateUpdate: (table, setExp, query) ->
    @ensureColumns table, setExp
    setGen = @criteriaQuery setExp, ', '
    if Object.keys(query).length == 0
      {stmt: "update #{@tableName(table.name)} set #{setGen}", args: setExp}
    else
      @ensureColumns table, query
      queryGen = @criteriaQuery query
      {stmt: "update #{@tableName(table.name)} set #{setGen} where #{queryGen}", args: _.extend({}, setExp, query)}
  normalizeRecord: (table, rec) ->
    console.log 'PostgresDriver.normalizeRecord', table.name, rec
    # postgres stores the columns case-insensitively, so we'll need to remap the records.
    obj = {}
    for col in table.columns
      lc = col.name.toLowerCase()
      console.log 'PostgresDriver.normalize', col.name, lc, rec[lc]
      if rec.hasOwnProperty(col.name)
        obj[col.name] = rec[col.name]
      else if rec.hasOwnProperty(lc)
        obj[col.name] = rec[lc]
      else
        throw new Error("PostgresDriver.normalizeRecord:unknown_column: #{col.name}")
    obj
  prepareSpecial: (key, val) ->
    if typeof(val) == 'string'
      @prepare key, (args, cb) ->
        @query val, args, cb
    else
      throw new Error("PostgresDriver.prepareSpecial:unsupported_query_type: #{val}")
  supports: (key) ->
    if key == 'in'
      true
    else
      false

DBConnect.register 'postgres', PostgresDriver

module.exports = PostgresDriver


