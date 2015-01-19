/**
 * Created by joachimvh on 7/01/2015.
 */

// mostly copy/paste of ldf-client with additional debug code

var ldf = require('../../ldf-client');

// Retrieve and check arguments
var args = require('minimist')(process.argv.slice(2));
if (args.listformats)
  return Object.keys(ldf.SparqlResultWriter.writers).forEach(function (t) { console.log(t); });
if (!(args.q || args.f) && args._.length < 1 || args._.length > 2 || args.h || args.help) {
  console.error('usage: ldf-client [startFragment] query.sparql [-c config.json] ' +
  '[-t application/json] [-l logLevel] [--help]');
  console.error('       ldf-client --listformats (Lists supported result formats for -t)');
  return process.exit(1);
}

// Load main libraries (postponed as to here for speed)
var fs = require('fs'),
  path = require('path'),
  N3 = require('n3'),
  Logger = ldf.Logger;

// Parse and initialize configuration
var configFile = args.c ? args.c : path.join(__dirname, '../../config-default.json'),
  config = JSON.parse(fs.readFileSync(configFile, { encoding: 'utf8' })),
  queryFile = args.f || args.q || args._.pop(),
  startFragment = args._.pop() || config.startFragment,
  query = args.q || (args.f || fs.existsSync(queryFile) ? fs.readFileSync(queryFile, 'utf8') : queryFile),
  mimeType = args.t || 'application/json';
config.algorithm = args.a || '1';

var logname = 'eval_algorithm' + config.algorithm + '.log';
fs.appendFileSync(logname, query.replace(/(\r\n|\n|\r)/gm," ") + '\n');

// Configure logging
Logger.setLevel(args.l || 'warning');

var start = new Date();
var DEBUGfirstTime, DEBUGtime, DEBUGfirstHttp, DEBUGhttp, DEBUGtotal = 0;

setTimeout(function () {
  DEBUGtime = new Date() - start;
  DEBUGhttp = config.fragmentsClient._httpClient.DEBUGcalls;
  fs.appendFileSync(logname, '!' + [DEBUGfirstTime, DEBUGfirstHttp, DEBUGtime, DEBUGhttp, DEBUGtotal, 'TIMEOUT'].join(';') + '\n');
  process.exit();
}, 5*60 *1000);

// Execute the query and output its results
config.fragmentsClient = new ldf.FragmentsClient(startFragment, config);
try {
  var sparqlIterator = new ldf.SparqlIterator(query, config), writer;
  switch (sparqlIterator.queryType) {
  // Write JSON representations of the rows or boolean
  case 'ASK':
  case 'SELECT':
    writer = new ldf.SparqlResultWriter(mimeType, sparqlIterator);
    writer.on('data', function (data) {
      if (data.length > 3)
        DEBUGtotal++;
      if (!DEBUGfirstTime && data.length > 3) {
        DEBUGfirstTime = new Date() - start;
        DEBUGfirstHttp = config.fragmentsClient._httpClient.DEBUGcalls;
      }
      process.stdout.write(data);
    });
    break;
  // Write an RDF representation of all results
  case 'CONSTRUCT':
  case 'DESCRIBE':
    writer = new N3.Writer(process.stdout, config);
    sparqlIterator.on('data', function (triple) { writer.addTriple(triple); })
                  .on('end',  function () { writer.end(); });
    break;
  default:
    throw new ldf.SparqlIterator.UnsupportedQueryError(query);
  }

  // Report an error's stack trace
  sparqlIterator.on('error', function (error) {
    console.error('ERROR: An error occured during query execution.\n');
    console.error(error.stack);
  });

  writer.on('end', function () {
    DEBUGtime = new Date() - start;
    DEBUGhttp = config.fragmentsClient._httpClient.DEBUGcalls;
    fs.appendFileSync(logname, '!' + [DEBUGfirstTime, DEBUGfirstHttp, DEBUGtime, DEBUGhttp, DEBUGtotal].join(';') + '\n');
    process.stdout.flush(); // TODO: find out why this is necessary (only with GraphIterator?)
    // TODO: ^ actually throws an error since that function doesn't exist :D. Probably should add a better solution.
  });
}
// Report a synchronous error
catch (error) {
  console.error('ERROR: Query execution could not start.\n');
  switch (error.name) {
    case 'InvalidQueryError':
    case 'UnsupportedQueryError':
      console.error(error.message);
      break;
    default:
      console.error(error.stack);
  }
}
