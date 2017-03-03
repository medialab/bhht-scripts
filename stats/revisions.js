/**
 * Revision Stats Script
 * ======================
 *
 * Script aiming at hitting the Wikipedia SQL database to retrieve some
 * aggregated statistics related to page revisions.
 */
const mysql = require('mysql'),
      yesql = require('yesql'),
      config = require('./config.default.json');

const QUERIES = yesql('./sql/');

const CONNECTION = mysql.createConnection({
  host: 'enwiki.labsdb',
  database: 'enwiki_p',
  user: config.user,
  password: config.password
});

CONNECTION.connect();

CONNECTION.query(QUERIES.countRevisionsForMultiplePages, [[50, 51]], (err, results, fields) => {
  console.log(err, results, fields);
});

CONNECTION.end();
