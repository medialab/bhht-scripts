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
  host: 'localhost',
  user: config.user,
  password: config.password
});
