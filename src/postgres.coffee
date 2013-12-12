
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
    name.toLowerCase()
  connect: (cb) ->
    # using the pool method by default.
    postgres.connect @connString(), (err, client, done) =>
      if err
        cb err
      else
        @inner = client
        @done = done
        cb null, @
  # what is it that I want to do with the tables???
  _query: (stmt, args, cb) ->
    # we'll need to parse the query to convert $key to $n
    {stmt, args} = @parseStmt stmt, args
    @inner.query stmt, args, cb
  parseStmt: (stmt, args) ->
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
    vals = []
    for key, val of args
      keys.push key
      vals.push val
    columnText = keys.join(', ')
    phText = ("$#{i}" for i in [1..vals.length]).join(', ')
    {stmt: "insert into #{table.name} (#{columnText}) select #{phText}", args: vals}
  criteriaQuery: (query, sep = ' and ', i = 1) ->
    criteria = []
    vals = []
    for keys, val of args
      criteria.push "#{key} = $#{i++}"
      vals.push val
    {stmt: criteria.join(sep), args: vals}
  generateDelete: (table, query) ->
    @ensureColumns table, query
    {stmt, args} = @criteriaQuery query
    if args.length == 0
      {stmt: "delete from #{table.name}", args: args}
    else
      {stmt: "delete from #{table.name} where #{stmt}", args: args}
  generateSelect: (table, query) ->
    @ensureColumns table, query
    {stmt, args} = @criteriaQuery query
    if args.length == 0
      {stmt: "select * from #{table.name}", args: args}
    else
      {stmt: "select * from #{table.name} where #{stmt}", args: args}
  generateUpdate: (table, setExp, query) ->
    @ensureColumns table, setExp
    @ensureColumsn table, query
    setGen = @criteriaQuery query, ', '
    queryGen = @criteriaQuery query, ' and ', setGen.args.length + 1
    if queryGen.args.length == 0
      {stmt: "update #{table.name} set #{setGen.stmt}", args: setGen.args}
    else
      stmt: "update #{table.name} set #{setGen.stmt} where #{queryGen.stmt}"
      args: setGen.args.concat(queryGen.args)


DBConnect.register 'postgres', PostgresDriver

module.exports = PostgresDriver


