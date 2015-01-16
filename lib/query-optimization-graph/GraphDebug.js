/**
 * Created by joachimvh on 19/08/2014.
 */

var path = require('path'),
    fs = require('fs'),
    FragmentsClient = require('../../ldf-client').FragmentsClient,
    Iterator = require('../iterators/Iterator'),
    SparqlParser = require('sparqljs').Parser,
    _ = require('lodash'),
    LDFClustering = require('./LDFClustering'),
    ClusteringAlgorithm = require('./ClusteringAlgorithm'),
    Logger = require ('../util/Logger'),
    TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
    N3 = require('n3'),
    LDFClusteringStream = require('./LDFClusteringStream'),
    ClusteringController = require('./ClusteringController'),
    rdf = require('../util/RdfUtil'),
    HttpClient = require('../util/HttpClient'),
    NeDBStoreInterface = require('./NeDBStoreInterface'),
    N3StoreInterface = require('./N3StoreInterface'),
    RDFStoreInterface = require('./RDFStoreInterface'),
    request = require('request'),
    spawn = require('child_process').spawn;

var configFile = path.join(__dirname, '../../config-default.json'),
    //configFile = path.join(__dirname, '../../../config-gtfs.json'),
    config = JSON.parse(fs.readFileSync(configFile, { encoding: 'utf8' })),
    //query = 'SELECT ?p ?c WHERE { ?p a <http://dbpedia.org/ontology/Artist>. ?p <http://dbpedia.org/ontology/birthPlace> ?c. ?c <http://xmlns.com/foaf/0.1/name> "York"@en. }';
    //query = 'SELECT ?p ?c WHERE { ?p a <http://dbpedia.org/ontology/Architect>. ?p <http://dbpedia.org/ontology/birthPlace> ?c. ?c <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Capitals_in_Europe>. }';
    query = 'SELECT ?clubName ?playerName WHERE {   ?club a dbpedia-owl:SoccerClub;         dbpedia-owl:ground ?city;         rdfs:label ?clubName.   ?player dbpedia-owl:team ?club;           dbpedia-owl:birthPlace ?city;           rdfs:label ?playerName.   ?city dbpedia-owl:country dbpedia:Spain. }';
    //query = 'SELECT * WHERE { ?e <http://dbpedia.org/ontology/series> <http://dbpedia.org/resource/The_Sopranos> . ?e  <http://dbpedia.org/ontology/releaseDate>    ?date . ?e <http://dbpedia.org/ontology/episodeNumber>  ?number . ?e <http://dbpedia.org/ontology/seasonNumber>   ?season }';
    //query = 'SELECT * WHERE {  ?company  a <http://dbpedia.org/ontology/Organisation>  . ?company  <http://dbpedia.org/ontology/foundationPlace>  <http://dbpedia.org/resource/California> . ?product  <http://dbpedia.org/ontology/developer> ?company . ?product  a <http://dbpedia.org/ontology/Software> }';
    //query = 'SELECT ?name ?birth ?description ?person WHERE { ?person dbpedia-owl:birthPlace dbpedia:Berlin . ?person <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:German_musicians> . ?person dbpedia-owl:birthDate ?birth . ?person foaf:name ?name . ?person rdfs:comment ?description . }';
    //query = 'SELECT * WHERE { ?person a dbpedia-owl:Artist; rdfs:label ?name; dbpedia-owl:birthPlace ?city. ?city rdfs:label ?cityName; rdfs:label "Ghent"@en; dbpedia-owl:country ?country. ?country rdfs:label "Belgium"@en. }';
config.logger = new Logger("HttpClient");
//config.logger.disable();
config.fragmentsClient = new FragmentsClient(config.startFragment, config);

//query = fs.readFileSync('../gtfsr1.sparql', { encoding: 'utf8' }); // different root?

query = new SparqlParser(config.prefixes).parse(query);
var triples = _.filter(query.where, function(patterns) { return patterns.type === 'bgp'; })[0].triples;

var logger = new Logger("DEBUG");

Logger.setLevel('INFO');
//Logger.disable();
// removed for easier debugging
//triples.splice(5, 1);
//triples.splice(2, 1);
//ClusteringController.create(triples, config, function (controller) {
  //controller.start();
  //var paths = controller.getAllPaths(_.find(controller.nodes, {pattern: {subject:'?player', object:'?club'}}), _.find(controller.nodes, {pattern: {subject:'?player', object:'?city'}}));
  //logger.info(_.map(paths, function (path) { return _.pluck(path, 'pattern'); }));
//});

Logger.setLevel('warning');
//var queryFile = '/host/projects/ldf/.data/access.log-20121101-bgps.log';
var queryFile = 'BenchmarkQueries.txt';
var input = fs.createReadStream(queryFile);
var reading = false;
var queries = [];
var startID = 24;
var id = startID;
readLines(input, 
  function (line) {
    queries.push(line);
    if (startID > 1) {
      startID--;
    } else {
        if (!reading)
          readQueries();
    }
  }
);

function next () {
  queries.shift();
  if (queries.length > 0)
    readQuery();
  else
    reading = false;
}

httpCallback = function(response) {
  response.on('data', function (chunk) {
    str += chunk;
  });

  response.on('end', function () {
    console.log(req.data);
    console.log(str);
  });
}

function readQueries () {
  reading = true;
  var algorithm = 1;
  var i = 0;
  readQuery2(id, algorithm, function callback () {
    ++i;
    if (i >= 1) {
      i = 0;
      algorithm++;
    }
    if (algorithm > 2) {
      algorithm = 1;
      ++id;
    }
    if (id > 25)
      return;
    readQuery2(id, algorithm, callback);
  });
}

function readQuery2 (id, algorithm, callback) {
  if (false /*id === 15 || id === 14 || id == 15 || id === 22 || id === 25*/) {
    console.log('skipping query ' + id);
    return setImmediate(callback);
  }
  reading = true;
  var clientPath = path.join(__dirname, 'EvalRunner-client');
  var configFile = 'config-bsbm.json';
  var clientArgs = [clientPath, '-c', configFile];console.log('starting query ' + id);
  var query = queries[id-1];
  query = query.split('\t');
  query = query[query.length-1];
  var start = new Date();
  var client = spawn(process.argv[0], clientArgs.concat(['-q', query, '-a', algorithm]));
  // timeout
  var timeout = setTimeout(function () {
    console.log('ERROR TIMEOUT');
    client.kill();
  }, 1*60*60*1000); // 1h
  
  var listened = false;
  client.stdout.once('data', function (data) {
    listened = true;
  });
  client.stderr.once('data', function (data) {
    if (!listened)
      console.log('ERROR ' + data);
    listened = true;
  });
  
  var ended = false;
  client.stdout.on('end', function () { ended ? end() : ended = true; });
  client.stderr.on('end', function () { ended ? end() : ended = true; });
  function end () {
    clearTimeout(timeout);
    console.log('done ' + (new Date() - start));
    setImmediate(callback);
  }
}

function readQuery (id, algorithm, callback) {
  if (false /*id === 15 || id === 14 || id == 15 || id === 22 || id === 25*/) {
    console.log('skipping query ' + id);
    return setImmediate(callback);
  }
  console.log('starting query ' + id);
  reading = true;
  var query = queries[id-1];
  query = query.split('\t');
  query = query[query.length-1];
  var url = 'http://localhost:3000/sparql?algorithm=' + algorithm +'&query=' + encodeURIComponent(query);
  var start = new Date();
  var options = {
    url: url,
    timeout: 0
  };
  var timeout = null;
  var req = request.get(options, function (error, response, body) {
    if (timeout)
      clearTimeout(timeout);
    if (error) {
      console.log('ERROR' + ' ' + (new Date() - start));
      console.log(error);
    } else if (response.statusCode !== 200) {
      console.log(response.statusCode + ' ' + (new Date() - start));
      console.log(body);
    } else {
      console.log(response.statusCode + ' ' + (new Date() - start));
    }
    setImmediate(callback);
  });
  // really force timeout
  /*timeout = setTimeout(function () {
    req.emit('error', 'TIMEOUT');
    req.abort();
    timeout = null;
  }, options.timeout);*/
}

// http://stackoverflow.com/questions/6831918/node-js-read-a-text-file-into-an-array-each-line-an-item-in-the-array
function readLines(input, func, end) {
  var remaining = '';

  input.on('data', function(data) {
    remaining += data;
    var index = remaining.search(/[\n\r]/);
    while (index > -1) {
      var line = remaining.substring(0, index);
      remaining = remaining.substring(index+1);
      func(line);
      index = remaining.search(/[\n\r]/);
    }
  });

  input.on('end', function() {
    if (remaining.length > 0) {
      func(remaining);
    }
    if (end) end();
  });
}

//var bgps = 0;
//function isBGP (group) {
//  if (group.type ==='bgp') {
//    bgps += group.triples.length;
//    return true;
//  }
//  if (group.type ==='filter')
//    return true;
//  if (group.type === 'group')
//    return _.every(group.patterns, isBGP);
//  return false;
//}
//
//var count = 0;
//var input = fs.createReadStream('C:/Users/jvherweg/Desktop/dbpedia-3.8-logs/access.log-20121101');
//readLines(input,
//  function (line) {
//    var start = line.indexOf('query=');
//    var end = line.indexOf('"', start);
//    var end2 = line.indexOf('&', start);
//    end = end2 < 0 ? end : end2;
//    line = line.substring(start+6, end);
//    line = decodeURIComponent(line).replace(/\+/g, ' ');
//    line = line.replace(/(?:\r\n|\r|\n)/g, ' ');
//    line = line.toLowerCase();
//
//    var roughParse = line.indexOf('union') >= 0 || line.indexOf('optional') >= 0|| line.indexOf('filter') >= 0;
//
//    if (!roughParse) {
//      try {
//        var parser = new SparqlParser().parse(line);
//        bgps = 0;
//        if (_.every(parser.where, isBGP) && bgps > 1) {
//          logger.info(bgps);
//          logger.info(line);
//          fs.appendFileSync('access.log-20121101-bgps.log', line + '\n');
//        }
//      } catch (e) { }
//    }
//    if (++count % 1000 === 0)
//      console.log(count);
//  },
//  function () { console.log('end'); }
//);

return 0;
