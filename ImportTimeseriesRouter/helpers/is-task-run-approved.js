const extract = require('../../Shared/extract')

module.exports = async function isTaskRunApproved (context, preparedStatement, message, throwExceptionOnNonMatch) {
  const isMadeCurrentManuallyMessage = 'is made current manually'
  // Test for automatic and manual task run approval.
  const taskRunApprovedRegex = new RegExp(`(?:Approved\\:?\\s*)True|False|${isMadeCurrentManuallyMessage}`, 'i')
  const taskRunApprovedText = 'task run approval status'
  const taskRunApprovedString = await extract(context, message, taskRunApprovedRegex, 1, 0, taskRunApprovedText, preparedStatement, throwExceptionOnNonMatch)
  return taskRunApprovedString && !!taskRunApprovedString.match(new RegExp(`true|${isMadeCurrentManuallyMessage}`, 'i'))
}
