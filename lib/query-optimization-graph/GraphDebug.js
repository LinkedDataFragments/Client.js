/**
 * Created by joachimvh on 19/08/2014.
 */

var path = require('path'),
    fs = require('fs'),
    FragmentsClient = require('../../ldf-client').FragmentsClient,
    Iterator = require('../iterators/Iterator'),
    SparqlParser = require('sparql-parser'),
    _ = require('lodash'),
    LDFClustering = require('./LDFClustering'),
    ClusteringAlgorithm = require('./ClusteringAlgorithm'),
    Logger = require ('../util/Logger'),
    TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
    rdfstore = require('rdfstore'),
    N3 = require('N3'),
    LDFClusteringStream = require("./LDFClusteringStream");

//Logger.disable();

var configFile = path.join(__dirname, '../../config-default.json'),
    //configFile = path.join(__dirname, '../../../config-gtfs.json'),
    config = JSON.parse(fs.readFileSync(configFile, { encoding: 'utf8' })),
    query = 'SELECT ?p ?c WHERE { ?p a <http://dbpedia.org/ontology/Artist>. ?p <http://dbpedia.org/ontology/birthPlace> ?c. ?c <http://xmlns.com/foaf/0.1/name> "York"@en. }';
    //query = 'SELECT ?p ?c WHERE { ?p a <http://dbpedia.org/ontology/Architect>. ?p <http://dbpedia.org/ontology/birthPlace> ?c. ?c <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Capitals_in_Europe>. }';
    //query = 'SELECT ?clubName ?playerName WHERE {   ?club a dbpedia-owl:SoccerClub;         dbpedia-owl:ground ?city;         rdfs:label ?clubName.   ?player dbpedia-owl:team ?club;           dbpedia-owl:birthPlace ?city;           rdfs:label ?playerName.   ?city dbpedia-owl:country dbpedia:Spain. }';
config.logger = new Logger("HttpClient");
//config.logger.disable();
config.fragmentsClient = new FragmentsClient(config.startFragment, config);

//query = fs.readFileSync('../gtfsr1.sparql', { encoding: 'utf8' }); // different root?

query = new SparqlParser(config.prefixes).parse(query);
var triples = _.filter(query.patterns, function(patterns) { return patterns.type === 'BGP'; })[0].triples;
//console.log(triples);

var logger = new Logger("DEBUG");

console.time("DONE");
var clustering = new LDFClustering(config, triples);
var algorithm = new ClusteringAlgorithm(clustering);
//algorithm.run(function(result) {
//  //logger.info(result);
//  //var f = _.bind(clustering._getVariableBindings, clustering);
//  //_.delay(f, 1000, '?p');
//  console.timeEnd("DONE");
//  //var g = _.bind(clustering.store.graph, clustering.store);
//  //_.delay(g, 3000, "http://example.org/clustering", function (success, result) { console.log(result.triples); });
//});

var tripleIterator = new TriplePatternIterator(Iterator.single({}), { subject: '?s', predicate: 'http://dbpedia.org/ontology/birthPlace', object: '?o' }, config);
//tripleIterator.toArray(function (error, items) {
//  console.log(items);
//});
//var temp = tripleIterator.read();
//if (!temp) {
//  tripleIterator.on('readable', function TEST() {
//    tripleIterator.removeListener('readable', TEST);
//    while (temp = tripleIterator.read())
//      logger.info(temp);
//  });
//}

//LDFClusteringStream.algorithm(config, triples);
LDFClusteringStream.init2(triples, config);
//tripleIterator.on('data', function TEST(val) {
//  //logger.info(val);
//  tripleIterator.removeListener('data', TEST);
//  setImmediate(function () { tripleIterator.on('data', TEST); });
//});
//console.log(_.intersection([{a:'b'}], [{a:'b'}]));

return 0;