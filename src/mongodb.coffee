mongodb = require 'mongodb'
DBConnect = require './dbconnect'
_ = require 'underscore'

class MongoDBDriver extends DBConnect
  @defaultOptions:
    host: '127.0.0.1'
    port: 27017
    database: 'test'
  connect: (cb) ->
    {host, port, database, queries} = @args
    try
      server = new mongodb.Server host or '127.0.0.1', port or 27017
      conn = new mongodb.Db database or 'test', server
      conn.open (err, inner) =>
        if err
          conn.close()
          cb err
        else
          @inner = inner
          cb null, @
    catch e
      console.log 'ERROR: MongoConnection.connect', e
      cb e
  disconnect: (cb) ->
    try
      @inner.close()
      cb null
    catch e
      cb e
  # query:
  # {insert: 'table', args: [ list_of_recs ] }
  # {update: 'table', $set: <set_exp>, query: <query_exp> }
  # {select: 'table', query: <query_exp> }
  # {delete: 'table', query: <query_exp> }
  _query: (stmt, args, cb) ->
    if arguments.length == 2
      cb = args
      args = {}
    if not (stmt instanceof Object)
      throw new Error("MongodBDriver.query_invalid_adhoc_query: #{stmt}")
    if stmt.insert # an insert statement.
      try
        @inner.collection(stmt.insert).insert stmt.args, {safe: true}, (err, res) ->
          if err
            cb err
          else
            cb null, res
      catch e
        cb e
    else if stmt.select
      try
        if stmt.query instanceof Object
          @inner.collection(stmt.select).find(stmt.query or {}).toArray (err, recs) ->
            if err
              cb err
            else
              cb null, recs
        else
          @inner.collection(stmt.select).find().toArray (err, recs) ->
            if err
              cb err
            else
              cb null, recs
      catch e
        cb e
    else if stmt.selectOne
      try
        if stmt.query instanceof Object
          @inner.collection(stmt.selectOne).find(stmt.query or {}).toArray (err, recs) ->
            if err
              cb err
            else
              cb null, if recs.length > 0 then recs[0] else null
        else
          @inner.collection(stmt.selectOne).find().toArray (err, recs) ->
            if err
              cb err
            else
              cb null, if recs.length > 0 then recs[0] else null
      catch e
        cb e
    else if stmt.update
      try
        @inner.collection(stmt.update).update stmt.query or {}, {$set: stmt.$set}, {safe: true, multi: true}, (err, res) ->
          if err
            cb err
          else
            cb null, res
      catch e
        cb e
    else if stmt.delete
      try
        @inner.collection(stmt.delete).remove stmt.query, {safe: true}, (err, res) ->
          if err
            cb err
          else
            cb null, res
      catch e
        cb e
    else if stmt.save
      try
        @inner.collection(stmt.save).save stmt.args, {safe: true}, (err, res) ->
          if err
            cb err
          else
            cb null, res
      catch e
        cb e
    else
      cb new Error("MongoDBDriver.query_unsupported_adhoc_query: #{stmt}")
  prepareSpecial: (key, args) ->
    if _.find(['select', 'selectOne', 'delete', 'update', 'insert', 'save'], ((key) -> args.hasOwnProperty(key)))
      @prepare key, @prepareStmt args
    else
      throw new Error("MongoDBDriver.unknown_prepare_special_args: #{JSON.stringify(args)}")
  prepareStmt: (stmt) ->
    (args, cb) ->
      normalized = @mergeQuery stmt, args
      @_query normalized, cb
  mergeQuery: (stmt, args) ->
    helper = (obj, args) ->
      if obj instanceof Object
        res = {}
        for key, val of obj
          if obj.hasOwnProperty(key)
            if val.match /^:/
              if args.hasOwnProperty(val.substring(1))
                res[key] = args[val.substring(1)]
              else
                res[key] = helper(val, args)
        res
      else
        obj
    if stmt.insert
      {insert: stmt.insert, args: args}
    else if stmt.select
      {select: stmt.select, query: helper(stmt.query, args)}
    else if stmt.selectOne
      {selectOne: stmt.selectOne, query: helper(stmt.query, args)}
    else if stmt.delete
      {delete: stmt.delete, query: helper(stmt.query, args)}
    else if stmt.update
      {update: stmt.update, $set: helper(stmt.$set, args), query: helper(stmt.query, args)}
    else if stmt.save
      {save: stmt.save, args: args}
    else
      throw new Error("MongoDBDriver.mergeQuery_unsupported_stmt: #{JSON.stringify(stmt)}")




DBConnect.register 'mongo', MongoDBDriver

module.exports = MongoDBDriver

