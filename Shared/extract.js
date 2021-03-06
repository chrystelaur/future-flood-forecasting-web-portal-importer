const createStagingException = require('./create-staging-exception')

module.exports = async function (context, message, regex, expectedNumberOfMatches, matchIndexToReturn, errorMessageSubject, preparedStatement) {
  const matches = regex.exec(message)
  // If the message contains the expected number of matches from the specified regular expression return
  // the match indicated by the caller.
  if (matches && matches.length === expectedNumberOfMatches) {
    return Promise.resolve(matches[matchIndexToReturn])
  } else {
    // If regular expression matching did not complete successfully, the message is not in an expected
    // format and cannot be replayed. In this case intervention is needed so create a staging
    // exception.
    return createStagingException(context, preparedStatement, message, `Unable to extract ${errorMessageSubject} from message`)
  }
}
