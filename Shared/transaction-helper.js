const Connection = require('../Shared/connection-pool')
const sql = require('mssql')

module.exports = {
  doInTransaction: async function (fn, context, errorMessage, isolationLevel, ...args) {
    const connection = new Connection()
    const pool = connection.pool
    const request = new sql.Request(pool)

    let transaction

    try {
      sql.on('error', err => {
        context.log.error(err)
        throw err
      })
      // Begin the connection to the DB and ensure the connection pool is ready
      await pool.connect()
      await request.batch(`set lock_timeout ${process.env['SQLDB_LOCK_TIMEOUT'] || 6500};`)
      // The transaction is created immediately for use
      transaction = new sql.Transaction(pool)

      if (isolationLevel) {
        await transaction.begin(isolationLevel)
      } else {
        await transaction.begin()
      }

      // Call the function to be executed in the transaction passing
      // through the transaction, context and arguments from the caller.
      return await fn(transaction, context, ...args)
    } catch (err) {
      context.log.error(`Transaction failed: ${errorMessage} ${err}`)
      if (transaction._aborted) {
        context.log.warn('The transaction has been aborted.')
      } else {
        await transaction.rollback()
        context.log.warn('The transaction has been rolled back.')
      }
      throw err
    } finally {
      try {
        if (transaction && !transaction._aborted && !transaction._rollbackRequested) {
          await transaction.commit()
        }
      } catch (err) { context.log.error(`Transaction-helper cleanup error: '${err.message}'.`) }
      try {
        if (pool) {
          await pool.close()
        }
      } catch (err) { context.log.error(`Transaction-helper cleanup error: '${err.message}'.`) }
    }
  },
  executePreparedStatementInTransaction: async function (fn, context, transaction, ...args) {
    let preparedStatement
    try {
      preparedStatement = new sql.PreparedStatement(transaction)
      // Call the function that prepares and executes the prepared statement passing
      // through the arguments from the caller.
      return await fn(context, preparedStatement, ...args)
    } finally {
      try {
        if (preparedStatement && preparedStatement.prepared) {
          await preparedStatement.unprepare()
        }
      } catch (err) { context.log.error(err) }
    }
  }
}
