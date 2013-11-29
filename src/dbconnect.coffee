_ = require 'underscore'
uuid = require './uuid'

class DBConnect
  @connTypes: {}
  @register: (type, connector) ->
    if @connTypes.hasOwnProperty(type)
      throw new Error("DBConnect.register_type_exists: #{type}")
    @connTypes[type] = connector
  @hasType: (type) ->
    if @connTypes.hasOwnProperty(type)
      @connTypes[type]
    else
      undefined
  @inners: {}
  @setup: (args) ->
    if @inners.hasOwnProperty(args.name)
      throw new Error("DBConnect.setup_connection_exists: #{args.name}")
    if not @connTypes.hasOwnProperty(args.type)
      throw new Error("DBConnect.unknown_type: #{args.type}")
    @inners[args.name] = args
    if args.hasOwnProperty('module')
      args.loader = require(args.module)
  @make: (args) ->
    if not @inners.hasOwnProperty(args)
      throw new Error("DBConnect.unknownSetup: #{args}")
    else
      args = @inners[args]
      type = @connTypes[args.type]
      new type(args)
  @defaultOptions: {}
  constructor: (args) ->
    @args = _.extend {}, @constructor.defaultOptions, args
    @prepared = {}
    @currentUser = null
    @loadModule()
  loadModule: (loader = @args.loader) ->

    if loader instanceof Function
      loader @
    else if loader instanceof Object
      for key, val of loader
        if loader.hasOwnProperty(key)
          console.log 'dbconnect.loadModule', key, val
          if val instanceof Function
            @prepare key, val
          else
            @prepareSpecial key, val
    else # OK if no loader
      return
  connect: (cb) ->
  query: (stmt, args, cb) ->
    if @prepared.hasOwnProperty(stmt)
      @prepared[stmt] @, args, cb
    else
      @_query arguments...
  prepare: (key, func) ->
    if @prepared.hasOwnProperty(key)
      throw new Error("#{@constructor.name}.duplicate_prepare_stmt: #{key}")
    if @hasOwnProperty(key)
      throw new Error("#{@constructor.name}.duplicate_prepare_stmt: #{key}")
    if func instanceof Function
      @prepared[key] = func
      @[key] = func
    else
      throw new Error("#{@constructor.name}.invalid_prepare_stmt_not_a_function: #{func}")
  prepareSpecial: (args...) ->
    @prepare args...
  disconnect: (cb) ->
  open: (cb) ->
    @connect cb
  close: (cb) ->
    @disconnect cb
  uuid: () ->
    uuid.v4()

module.exports = DBConnect

