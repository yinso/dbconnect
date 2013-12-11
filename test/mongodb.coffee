DBConnect = require '../src/main'
schemaInit = require '../example/schema'

schema = new DBConnect.Schema('auth')

conn = null

DBConnect.setup
  name: 'test'
  type: 'mongo'
  module: '../example/mongodb'
  database: 'auth'
  schema: schemaInit(schema)

describe 'can connect', () ->
  it 'can connect', (done) ->
    try
      conn = DBConnect.make 'test'
      conn.open done
    catch e
      done e

  it 'can select', (done) ->
    try
      conn.query {select: 'user'}, (err, recs) ->
        test.ok () -> recs.length > 0
        done err
    catch e
      done e

  it 'can call prepared query', (done) ->
    try
      conn.getUser {login: 'yc'}, (err, recs) ->
        test.equal recs.length, 1
        done err
    catch e
      done e

  it 'can remove', (done) ->
    try
      conn.query {delete: 'test'}, done
    catch e

  it 'can insert', (done) ->
    try
      conn.query {insert: 'test', args: {abc: 1, id: 1}}, (err) ->
        if err
          done err
        else
          conn.query {select: 'test'}, (err, recs) ->
            if err
              done err
            else
              try
                test.equal recs.length, 1
                done null
              catch err
                done err
    catch e
      done e

  it 'can update', (done) ->
    try
      conn.query {update: 'test', $set: {abc: 2}, query: {id: 1}}, (err) ->
        if err
          done err
        else
          conn.query {select: 'test', query: {id: 1}}, (err, recs) ->
            if err
              done err
            else
              try
                test.equal recs.length, 1, "length == 1"
                test.equal recs[0].abc, 2, "abc should be 2 but is #{recs[0].abc}"
                done null
              catch err
                done err
    catch e
      done e

  it 'can save', (done) ->
    try
      conn.query {save: 'test', args: {abc: 2, id: 2}}, (err, recs) ->
        console.log err, recs
        done err
    catch e
      done e

  it 'can select via .select', (done) ->
    try
      conn.select 'User', {login: 'yc'}, (err, res) ->
        console.log 'conn.select', err, res
        done err
    catch e
      done e

  it 'can remove', (done) ->
    try
      conn.query {delete: 'test'}, done
    catch e

  it 'can disconnect', (done) ->
    try
      conn.close done
    catch e
      done e