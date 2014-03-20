/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var GraphPatternIterator = require('../../lib/iterators/GraphPatternIterator');

var Stream = require('stream').Stream,
    PassThrough = require('stream').PassThrough,
    EmptyIterator = require('../../lib/iterators/EmptyIterator'),
    SingleItemIterator = require('../../lib/iterators/SingleItemIterator'),
    TriplePatternIterator = require('../../lib/iterators/TriplePatternIterator'),
    FileFragmentsClient = require('../lib/FileFragmentsClient'),
    rdf = require('../../lib/rdf/RdfUtil'),
    _ = require('lodash');

var testClient = new FileFragmentsClient();

var patterns = {
  p_type_artist: rdf.triple('?p', rdf.RDF_TYPE, rdf.DBPEDIAOWL + 'Artist'),
  p_birthplace_c: rdf.triple('?p', rdf.DBPEDIAOWL + 'birthPlace', '?c'),
  c_name_york: rdf.triple('?c', rdf.FOAF + 'name', '"York"@en'),
  p_birthplace_york: { subject: '?p', predicate: rdf.DBPEDIAOWL + 'birthPlace', object: rdf.DBPEDIA + 'York' },
  p_birthplace_yorkontario: { subject: '?p', predicate: rdf.DBPEDIAOWL + 'birthPlace', object: rdf.DBPEDIA + 'York,_Ontario' },
};
var yorkQuery = [
  patterns.p_birthplace_c,
  patterns.c_name_york,
];
var artistQuery = [
  patterns.p_type_artist,
  patterns.p_birthplace_c,
  patterns.c_name_york,
];
testClient._metadata = {
  '$-type-artist':  { totalTriples: 500000 },
  '$-birthplace-$': { totalTriples: 1000000 },
  '$-name-york':    { totalTriples: 10 },
};

describe('GraphPatternIterator', function () {
  describe('The GraphPatternIterator module', function () {
    it('should be a function', function () {
      GraphPatternIterator.should.be.a('function');
    });

    it('should make GraphPatternIterator objects', function () {
      GraphPatternIterator(yorkQuery).should.be.an.instanceof(GraphPatternIterator);
    });

    it('should be a GraphPatternIterator constructor', function () {
      new GraphPatternIterator(yorkQuery).should.be.an.instanceof(GraphPatternIterator);
    });

    it('should make Stream objects', function () {
      GraphPatternIterator(yorkQuery).should.be.an.instanceof(Stream);
    });

    it('should be a Stream constructor', function () {
      new GraphPatternIterator(yorkQuery).should.be.an.instanceof(Stream);
    });
  });

  describe('when created with the empty graph', function () {
    var iterator = new GraphPatternIterator([]);
    it('should be a pass-through iterator', function () {
      iterator.should.be.an.instanceof(PassThrough);
    });
  });

  describe('when created with a single-element graph', function () {
    var triple = rdf.triple('?a', 'b', 'c'),
        iterator = new GraphPatternIterator([triple]);
    it('should be a triple pattern iterator', function () {
      iterator.should.be.an.instanceof(TriplePatternIterator);
    });
    it('should iterate over that triple pattern', function () {
      iterator.should.have.property('_pattern', triple);
    });
  });

  describe('when piped no bindings', function () {
    function createSource() { return EmptyIterator(); }

    describe('a GraphPatternIterator for the York query', function () {
      var iterator = new GraphPatternIterator(yorkQuery, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should return no bindings', function (done) {
        var expectedBindings = [];
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });

  describe('when piped a single non-overlapping bindings object', function () {
    function createSource() { return SingleItemIterator({ '?a': 'a' }); }

    describe('a GraphPatternIterator for the York query', function () {
      var iterator = new GraphPatternIterator(yorkQuery, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should be a stream of ?a/?p/?o bindings', function (done) {
        var yorkBindings = testClient.getBindingsByPattern(patterns.p_birthplace_york)
            .map(function (binding) {
              return { bindings: { '?a': 'a', '?c': rdf.DBPEDIA + 'York', '?p': binding.subject } };
            });
        var yorkOntarioBindings = testClient.getBindingsByPattern(patterns.p_birthplace_yorkontario)
            .map(function (binding) {
              return { bindings: { '?a': 'a', '?c': rdf.DBPEDIA + 'York,_Ontario', '?p': binding.subject } };
            });
        var expectedBindings = yorkOntarioBindings.concat(yorkBindings);
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });

  describe('when piped a single overlapping bindings object', function () {
    function createSource() { return SingleItemIterator({ '?a': 'a', '?c': rdf.DBPEDIA + 'York' }); }

    describe('a GraphPatternIterator for the York query', function () {
      var iterator = new GraphPatternIterator(yorkQuery, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should be a stream of matching ?a/?p/?o bindings', function (done) {
        var yorkBindings = testClient.getBindingsByPattern(patterns.p_birthplace_york)
            .map(function (binding) {
              return { bindings: { '?a': 'a', '?c': rdf.DBPEDIA + 'York', '?p': binding.subject } };
            });
        var expectedBindings = yorkBindings;
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });
});
