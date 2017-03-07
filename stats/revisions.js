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
      fs = require('fs'),
      _ = require('lodash');

/**
 * Helpers.
 */
function readHeadlessRow(row) {
  return {
    name: row[2]
  };
}

function hashRow(row) {
  return `${row.name}`;
}

function writeRow(row) {
  return row.map(item => {
    item = '' + item;

    if (/,/.test(item))
      item = '"' + item + '"';

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

let ALREADY_DONE_COUNT = 0;

// Reading the output file
touch.sync(OUTPUT);

// Function used to gather already done rows
function checkAlreadyDone(next) {
  const parser = csv.parse({delimiter: ','});

  return fs.createReadStream(OUTPUT, 'utf-8')
    .pipe(parser)
    .on('data', row => {
      ALREADY_DONE.add(hashRow(readHeadlessRow(row)));
    })
    .on('error', next)
    .on('end', () => {
      ALREADY_DONE_COUNT = ALREADY_DONE.size;

      console.log(`Already done ${ALREADY_DONE_COUNT} elements.`);

      return next();
    });
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
    password: CONFIG.password,
    supportBigNumbers: true,
    bugNumberStrings: true,
    dateStrings: true,
    multipleStatements: true
  });

  CONNECTION.connect();
}

function disconnect() {
  CONNECTION.end();
}

const runQuery = _.curry((query, ids, next) => {
  return CONNECTION.query(query, [ids], (err, results) => {
    if (err)
      return next(err);

    return next(null, results);
  });
});

function retrieveDataForIds(ids, index, next) {
  return async.parallel({
    count: runQuery(QUERIES.countRevisionsForMultiplePages, ids),
    minorEditCount: runQuery(QUERIES.countMinorEditRevisionsForMultiplePages, ids),
    distinctContributorsCount: runQuery(QUERIES.countDistinctContributorsForMultiplePages, ids),
    firstRevision: runQuery(QUERIES.firstRevisionForMultiplePages, ids),
    // lengthStats: next => {
    //   return CONNECTION
    // }
  }, (err, results) => {
    if (err)
      return next(err);

    const resultsPerRow = {};

    for (const name in results) {
      const result = results[name];

      result.forEach(row => {
        resultsPerRow[row.id] = resultsPerRow[row.id] || {};
        resultsPerRow[row.id][name] = row;
      });
    }

    return next(null, resultsPerRow);
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
        const filteredRows = rows.filter(row => {
          return !ALREADY_DONE.has(hashRow(row));
        });

        if (!filteredRows.length)
          return callback();

        const rowsIndex = _.keyBy(filteredRows, 'id');

        filteredRows.forEach(row => {
          ALREADY_DONE_COUNT++;

          console.log(`(${ALREADY_DONE_COUNT}) Processing "${row.name}"...`)
        });

        const ids = filteredRows.map(row => +row.id);

        return retrieveDataForIds(ids, rowsIndex, (err, data) => {
          if (err)
            return err;

          console.log(data);

          // Writing to output stream
          for (const id in data) {
            const line = [
              'en',
              id,
              rowsIndex[id].name,
              data[id].count.count,
              data[id].minorEditCount ? data[id].minorEditCount.count : 0,
              data[id].distinctContributorsCount ? data[id].distinctContributorsCount.count : 0,
              data[id].firstRevision ? (data[id].firstRevision.firstRevision || '').slice(0, 8) : '',
              data[id].firstRevision ? data[id].firstRevision.bytes : 0
            ];

            output.write(writeRow(line) + '\n');
          }

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
