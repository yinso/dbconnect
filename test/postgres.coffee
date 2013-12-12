DBConnect = require '../src/main'
schemaInit = require '../example/schema'
uuid = require 'node-uuid'

schema = new DBConnect.Schema('auth')

conn = null

DBConnect.setup
  name: 'test2'
  type: 'postgres'
  database: 'test'
  schema: schemaInit(schema)

userArg = {login: 'test', email: 'testa.testing111@gmail.com', uuid: uuid.v4() }

user = null

describe 'postgresql test', () ->
  it 'can connect', (done) ->
    try
      conn = DBConnect.make 'test2'
      conn.connect (err, res) ->
        if err
          done err
        else
          done null
    catch e
      done e

  it 'can insert', (done) ->
    try
      conn.query "insert into test1 (col1, col2) values ($col1, $col2)", {col1: 1, col2: 2}, (err, res) ->
        console.log 'pg.insert', err, res
        done err
    catch e
      done e

  it 'can select', (done) ->
    try
      conn.query "select * from test1 where col1 = $col1", {col1: 1}, (err, res) ->
        console.log 'pg.select', err, res
        done err
    catch e
      done e

  it 'can update', (done) ->
    try
      conn.query "update test1 set col2 = $col2 where col1 = $col1", {col1: 1, col2: 3}, (err, res) ->
        console.log 'pg.update', err, res
        done err
    catch e
      done e

  it 'can delete', (done) ->
    try
      conn.query "delete from test1 where col2 = $col2", {col2: 3}, (err, res) ->
        console.log 'pg.delete', err, res
        done err
    catch e
      done e

  it 'can use prepare statement', (done) ->
    try
      conn.prepare 'insertTest', (args, cb) ->
        @query "insert into test1 (col1, col2) values ($col1, $col2)", args, cb
      conn.insertTest {col1: 1, col2: 2}, (err, res) ->
        done err
    catch e
      done e

  it 'can use prepare statement', (done) ->
    try
      conn.prepare 'selectTest', (args, cb) ->
        @query "select * from test1 where col1 = $col1", args, cb
      conn.selectTest {col1: 1}, done
    catch e
      done e

  it 'can use prepare statement', (done) ->
    try
      conn.prepare 'deleteTest', (args, cb) ->
        @query "delete from test1", args, cb
      conn.deleteTest {col1: 1, col2: 2}, (err, res) ->
        done err
    catch e
      done e
  it 'can delete', (done) ->
    try
      conn.query 'delete from user_t', {}, done
    catch e
      done e

  it 'can insert via .insert()', (done) ->
    try
      conn.insert 'User', {login: 'test', email: 'testa.testing111@gmail.com'}, (err, u) ->
        console.log 'user.insert', err, u
        if err
          done err
        else
          user = u
          done null
    catch e
      done e

  it 'can select via .selectOne()', (done) ->
    try
      conn.selectOne 'User', {email: 'testa.testing111@gmail.com'}, (err, u) ->
        if err
          done err
        else
          user = u
          done null
    catch e

  it 'can update via .update()', (done) ->
    try
      user.update {email: 'test@gmail.com'}, (err) ->
        if err
          done err
        else
          done null
    catch e
      done e

  it 'can delete via .delete()', (done) ->
    try
      user.delete (err) ->
        if err
          done err
        else
          done null
    catch e
      done e

  it 'can issue transaction', (done) ->
    try
      conn.beginTrans (err) ->
        if err
          done err
        else
          conn.query "insert into test1 (col1, col2) values ($col1, $col2)", {col1: 1, col2: 2}, (err) ->
            if err
              done err
            else
              conn.commit (err) ->
                if err
                  done err
                else
                  conn.query 'delete from test1', {}, (err) ->
                    if err
                      done err
                    else
                      done null
    catch e
      done e

  it 'can disconnect', (done) ->
    try
      conn.disconnect (err) ->
        done err
    catch e
      done e
