/**
 * Revision Stats Script
 * ======================
 *
 * Script aiming at hitting the Wikipedia SQL database to retrieve some
 * aggregated statistics related to page revisions.
 */
const mysql = require('mysql'),
      config = require('./config.json');

const CONNECTION = mysql.createConnection({
  host: 'enwiki.labsdb',
  database: 'enwiki_p',
  user: config.user,
  password: config.password
});

CONNECTION.connect();

CONNECTION.query('SELECT * FROM revision LIMIT 10', (err, results) => {
  console.log(err, results);
});

CONNECTION.end();
