const sql = require('mssql')

module.exports = async function (context, payload, description, preparedStatement) {
  try {
    await preparedStatement.input('payload', sql.NVarChar)
    await preparedStatement.input('description', sql.NVarChar)

    await preparedStatement.prepare(`
      insert into
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.staging_exception (payload, description)
      values
       (@payload, @description)
    `)

    const parameters = {
      payload: payload,
      description: description
    }

    await preparedStatement.execute(parameters)
  } catch (err) {
    context.log.error(err)
    throw err
  } finally {
    try {
      if (preparedStatement) {
        await preparedStatement.unprepare()
      }
    } catch (err) {
      context.log.error(err)
    }
  }
}