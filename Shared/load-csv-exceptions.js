const { executePreparedStatementInTransaction } = require('../Shared/transaction-helper')
const createCSVStagingException = require('./create-csv-staging-exception')

module.exports = async function loadExceptions (transaction, context, failedRows) {
  for (let i = 0; i < failedRows.length; i++) {
    await executePreparedStatementInTransaction(createCSVStagingException, context, transaction, `Non display group data`, failedRows[i].rowData, failedRows[i].errorMessage)
  }
}
