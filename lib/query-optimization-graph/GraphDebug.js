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
    LDFClusteringStream = require('./LDFClusteringStream'),
    ClusteringController = require('./ClusteringController');

//Logger.disable();

var configFile = path.join(__dirname, '../../config-default.json'),
    //configFile = path.join(__dirname, '../../../config-gtfs.json'),
    config = JSON.parse(fs.readFileSync(configFile, { encoding: 'utf8' })),
    //query = 'SELECT ?p ?c WHERE { ?p a <http://dbpedia.org/ontology/Artist>. ?p <http://dbpedia.org/ontology/birthPlace> ?c. ?c <http://xmlns.com/foaf/0.1/name> "York"@en. }';
    //query = 'SELECT ?p ?c WHERE { ?p a <http://dbpedia.org/ontology/Architect>. ?p <http://dbpedia.org/ontology/birthPlace> ?c. ?c <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Capitals_in_Europe>. }';
    query = 'SELECT ?clubName ?playerName WHERE {   ?club a dbpedia-owl:SoccerClub;         dbpedia-owl:ground ?city;         rdfs:label ?clubName.   ?player dbpedia-owl:team ?club;           dbpedia-owl:birthPlace ?city;           rdfs:label ?playerName.   ?city dbpedia-owl:country dbpedia:Spain. }';
config.logger = new Logger("HttpClient");
//config.logger.disable();
config.fragmentsClient = new FragmentsClient(config.startFragment, config);

//query = fs.readFileSync('../gtfsr1.sparql', { encoding: 'utf8' }); // different root?

query = new SparqlParser(config.prefixes).parse(query);
var triples = _.filter(query.patterns, function(patterns) { return patterns.type === 'BGP'; })[0].triples;
//console.log(triples);

var logger = new Logger("DEBUG");

//var clustering = new LDFClustering(config, triples);
//var algorithm = new ClusteringAlgorithm(clustering);
//algorithm.run(function(result) {
//  logger.info(result);
//  //var f = _.bind(clustering._getVariableBindings, clustering);
//  //_.delay(f, 1000, '?p');
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
//LDFClusteringStream.init2(triples, config);
//tripleIterator.on('data', function TEST(val) {
//  //logger.info(val);
//  tripleIterator.removeListener('data', TEST);
//  setImmediate(function () { tripleIterator.on('data', TEST); });
//});

//Logger.disable();
// removed for easier debugging
//triples.splice(5, 1);
//triples.splice(2, 1);
// TODO: chicken/egg solution: 2 types of values? ones to stabilize and ones to actually match data?
ClusteringController.create(triples, config, function (controller) {
//  _.find(controller.nodes, {pattern: {object:'http://dbpedia.org/ontology/Artist'}}).fixBindVar('?p');
//  _.find(controller.nodes, {pattern: {object:'?c'}}).fixBindVar('?c');
//  _.find(controller.nodes, {pattern: {subject:'?c'}}).fixBindVar(null);
//  _.find(controller.nodes, {pattern: {object:'http://dbpedia.org/ontology/Architect'}}).fixBindVar(null);
//  _.find(controller.nodes, {pattern: {object:'?c'}}).fixBindVar('?c');
//  _.find(controller.nodes, {pattern: {subject:'?c'}}).fixBindVar(null);
//  _.find(controller.nodes, {pattern: {object:'http://dbpedia.org/ontology/SoccerClub'}}).fixBindVar(null);
//  _.find(controller.nodes, {pattern: {subject:'?club', object:'?city'}}).fixBindVar(null);
//  _.find(controller.nodes, {pattern: {subject:'?club', object:'?clubName'}}).fixBindVar('?club');
//  _.find(controller.nodes, {pattern: {subject:'?player', object:'?club'}}).fixBindVar('?club');
//  _.find(controller.nodes, {pattern: {subject:'?player', object:'?city'}}).fixBindVar('?city');
//  _.find(controller.nodes, {pattern: {subject:'?player', object:'?playerName'}}).fixBindVar('?player');
//  _.find(controller.nodes, {pattern: {subject:'?city'}}).fixBindVar(null);
  controller.start();
  //var paths = controller.getAllPaths(_.find(controller.nodes, {pattern: {subject:'?player', object:'?club'}}), _.find(controller.nodes, {pattern: {subject:'?player', object:'?city'}}));
  //logger.info(_.map(paths, function (path) { return _.pluck(path, 'pattern'); }));
});

var grounds = [];
var cities = [];
var clubs = [];
var soccerPlayers = [];
var spanishCitizens = [];

//var delayedCallback = _.after(3, function () {
//  var uniqGrounds = _.uniq(_.pluck(grounds, '?o'));
//  var uniqCities = _.uniq(_.pluck(cities, '?s'));
//  var uniqThings = _.uniq(_.pluck(grounds, '?s'));
//  var uniqSoccerClubs = _.uniq(_.pluck(clubs, '?s'));
//  logger.info("CHECK");
//  logger.info("ground cities: " + _.size(uniqGrounds));
//  logger.info("spanish cities: " + _.size(uniqCities));
//  logger.info("ground clubs: " + _.size(uniqThings));
//  logger.info("soccer clubs: " + _.size(uniqSoccerClubs));
//  logger.info("ground spanish cities: " + _.size(_.intersection(uniqGrounds, uniqCities)));
//  logger.info("ground soccer clubs: " + _.size(_.intersection(uniqSoccerClubs, uniqThings)));
//  var spanishSoccerCities = [];
//  var spanishSoccerClubs = _.filter(uniqSoccerClubs, function (club) {
//    var groundMatches = _.filter(grounds, function (ground) { return ground['?s'] === club; });
//    if (_.isEmpty(groundMatches))
//      return false;
//    var groundCities = _.pluck(groundMatches, '?o');
//    var spanishMatches = _.filter(cities, function (city) { return _.contains(groundCities, city['?s']); });
//    spanishSoccerCities = spanishSoccerCities.concat(_.pluck(spanishMatches, '?s'));
//    return !_.isEmpty(spanishMatches);
//  });
//  spanishSoccerCities = _.uniq(spanishSoccerCities);
//  logger.info("spanish soccer clubs: " + _.size(spanishSoccerClubs));
//  logger.info(spanishSoccerClubs);
//  logger.info("spanish soccer cities: " + _.size(spanishSoccerCities));
//  logger.info(spanishSoccerCities);
//  var delayedCallback2 = _.after(_.size(spanishSoccerClubs) + _.size(spanishSoccerCities), function () {
//    logger.info("spanish soccer players in own city: ");
//    var players = _.filter(_.pluck(soccerPlayers, '?s'), function (player, idx) {
//      var club = soccerPlayers[idx]['?o'];
//      var cities = _.pluck(_.filter(spanishCitizens, function (citizen) { return citizen['?s'] === player; }), '?o');
//      var localGrounds = _.filter(grounds, function (ground) {
//        return ground['?s'] === club && _.contains(cities, ground['?o']);
//      });
//      return !_.isEmpty(localGrounds);
//    });
//    players = _.uniq(players);
//    logger.info(_.size(players));
//    logger.info(players);
//  });
//  _.each(spanishSoccerClubs, function (club) {
//    var iterator = new TriplePatternIterator(Iterator.single({}), { subject: '?s', predicate: 'http://dbpedia.org/ontology/team', object: club }, config);
//    iterator.toArray(function (error, items) {
//      items = _.map(items, function (item) {
//        item['?o'] = club;
//        return item;
//      });
//      soccerPlayers = soccerPlayers.concat(items);
//      delayedCallback2();
//    });
//  });
//  _.each(spanishSoccerCities, function (city) {
//    var iterator = new TriplePatternIterator(Iterator.single({}), { subject: '?s', predicate: 'http://dbpedia.org/ontology/birthPlace', object: city }, config);
//    iterator.toArray(function (error, items) {
//      items = _.map(items, function (item) {
//        item['?o'] = city;
//        return item;
//      });
//      spanishCitizens = spanishCitizens.concat(items);
//      delayedCallback2();
//    });
//  });
//});
//var groundIterator = new TriplePatternIterator(Iterator.single({}), { subject: '?s', predicate: 'http://dbpedia.org/ontology/ground', object: '?o' }, config);
//groundIterator.on('data', function groundData (val) {
//  grounds.push(val);
//  if (_.size(grounds) >= 15098-6198) {
//    groundIterator.removeListener('data', groundData);
//    delayedCallback();
//  }
//});
//var spainIterator = new TriplePatternIterator(Iterator.single({}), { subject: '?s', predicate: 'http://dbpedia.org/ontology/country', object: 'http://dbpedia.org/resource/Spain' }, config);
//spainIterator.on('data', function spainData (val) {
//  cities.push(val);
//  if (_.size(cities) >= 7363-0) {
//    spainIterator.removeListener('data', spainData);
//    delayedCallback();
//  }
//});
//var clubIterator = new TriplePatternIterator(Iterator.single({}), { subject: '?s', predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', object: 'http://dbpedia.org/ontology/SoccerClub' }, config);
//clubIterator.on('data', function spainData (val) {
//  clubs.push(val);
//  if (_.size(clubs) >= 15727-6427) {
//    clubIterator.removeListener('data', spainData);
//    delayedCallback();
//  }
//});


return 0;