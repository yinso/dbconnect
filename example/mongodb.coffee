module.exports =
  getUser:
    select: 'user'
    query: {login: ':login'}
