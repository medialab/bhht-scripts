/**
 * Revision Stats Script
 * ======================
 *
 * Script aiming at hitting the Wikipedia SQL database to retrieve some
 * aggregated statistics related to page revisions.
 */
const yargs = require('yargs'),
      async = require('async'),
      highland = require('highland'),
      mysql = require('mysql'),
      yesql = require('yesql'),
      touch = require('touch'),
      csv = require('csv'),
      fs = require('fs');

/**
 * Helpers.
 */
function hashRow(row) {
  return `${row.lang}§${row.name}`;
}

function writeRow(row) {
  return row.map(item => {
    item = '' + item;

    if (/,/.test(item))
      item = '"' + item '"';

    return item;
  }).join(',');
}

/**
 * CLI.
 */
const argv = yargs
  .usage('Usage: $0 --input [input] --output [output]')
  .option('options', {
    default: './config.json'
  })
  .option('i', {
    alias: 'input',
    demand: true
  })
  .option('o', {
    alias: 'output',
    demand: true
  })
  .help()
  .argv;

// Config
const CONFIG = require(argv.options);

// Queries
const QUERIES = yesql('./sql/');

/**
 * State.
 */
const ALREADY_DONE = new Set(),
      INPUT = argv.input,
      OUTPUT = argv.output;

// Reading the output file
touch.sync(OUTPUT);

// Function used to gather already done rows
function checkAlreadyDone(next) {
  const parser = csv.parse({delimiter: ',', columns: true});

  return fs.createReadStream(OUTPUT, 'utf-8')
    .pipe(parser)
    .on('data', row => {
      ALREADY_DONE.add(hashRow(row));
    })
    .on('error', next)
    .on('end', () => next());
}

/**
 * Database related functions.
 */
let CONNECTION;

function connect() {
  CONNECTION = mysql.createConnection({
    host: 'enwiki.labsdb',
    database: 'enwiki_p',
    user: CONFIG.user,
    password: CONFIG.password
  });

  CONNECTION.connect();
}

function disconnect() {
  CONNECTION.end();
}

function getRevisionCountsForIds(ids, next) {
  return CONNECTION.query(QUERIES.countRevisionsForMultiplePages, [ids], (err, results) => {
    if (err)
      return next(err);

    return next(null, results);
  });
}

/**
 * Process outline.
 */
connect();

async.series([
  checkAlreadyDone,
  function processRows(next) {
    const parser = csv.parse({delimiter: ',', columns: true});

    const output = fs.createWriteStream(OUTPUT, {
      flags: 'a+',
      defaultEncoding: 'utf-8'
    });

    const stream = fs.createReadStream(INPUT, 'utf-8').pipe(parser);

    highland(stream)
      .batch(100)
      .flatMap(highland.wrapCallback(function(rows, callback) {
        const ids = rows.map(row => +row.id);

        return getRevisionCountsForIds(ids, (err, result) => {
          if (err)
            return err;

          // Writing to output stream
          result.forEach((line, i) => {
            output.write(writeRow([
              'en',
              rows[i].id,
              rows[i].name,
              line.count
            ]) + '\n');
          });

          return callback();
        });
      }))
      .done(next);
  }
], err => {
  disconnect();

  if (err)
    return console.error(err);

  console.log('Done!');
});
