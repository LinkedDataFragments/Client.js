/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var GraphPatternIterator = require('../../lib/iterators/GraphPatternIterator');

var Iterator = require('../../lib/iterators/Iterator'),
    EmptyIterator = Iterator.EmptyIterator,
    SingleIterator = Iterator.SingleIterator,
    TriplePatternIterator = require('../../lib/iterators/TriplePatternIterator'),
    FileFragmentsClient = require('../lib/FileFragmentsClient'),
    rdf = require('../../lib/rdf/RdfUtil');

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
      GraphPatternIterator(null, yorkQuery).should.be.an.instanceof(GraphPatternIterator);
    });

    it('should be a GraphPatternIterator constructor', function () {
      new GraphPatternIterator(null, yorkQuery).should.be.an.instanceof(GraphPatternIterator);
    });

    it('should make Iterator objects', function () {
      GraphPatternIterator(null, yorkQuery).should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new GraphPatternIterator(null, yorkQuery).should.be.an.instanceof(Iterator);
    });
  });

  describe('A GraphPatternIterator created with the empty graph', function () {
    var iterator = new GraphPatternIterator(null, []);
    it('should be a pass-through iterator', function () {
      iterator.should.be.an.instanceof(Iterator.PassthroughIterator);
    });
  });

  describe('A GraphPatternIterator created with a single-element graph', function () {
    var triple = rdf.triple('?a', 'b', 'c'),
        iterator = new GraphPatternIterator(null, [triple]);
    it('should be a triple pattern iterator', function () {
      iterator.should.be.an.instanceof(TriplePatternIterator);
    });
    it('should iterate over that triple pattern', function () {
      iterator.should.have.property('_pattern', triple);
    });
  });

  describe('A GraphPatternIterator with an empty parent', function () {
    function createSource() { return Iterator.empty(); }

    describe('passed a GraphPatternIterator for the York query', function () {
      var iterator = new GraphPatternIterator(createSource(),
        yorkQuery, { fragmentsClient: testClient });
      it('should return no bindings', function (done) {
        var expectedBindings = [];
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });

  describe('A GraphPatternIterator passed a single non-overlapping bindings object', function () {
    function createSource() { return Iterator.single({ '?a': 'a' }); }

    describe('passed a GraphPatternIterator for the York query', function () {
      var iterator = new GraphPatternIterator(createSource(),
        yorkQuery, { fragmentsClient: testClient });
      it('should be an iterator of ?a/?p/?o bindings', function (done) {
        var yorkBindings = testClient.getBindingsByPattern(patterns.p_birthplace_york)
            .map(function (binding) {
              return { '?a': 'a', '?c': rdf.DBPEDIA + 'York', '?p': binding.subject };
            });
        var yorkOntarioBindings = testClient.getBindingsByPattern(patterns.p_birthplace_yorkontario)
            .map(function (binding) {
              return { '?a': 'a', '?c': rdf.DBPEDIA + 'York,_Ontario', '?p': binding.subject };
            });
        var expectedBindings = yorkOntarioBindings.concat(yorkBindings);
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });

  describe('when passed a single overlapping bindings object', function () {
    function createSource() { return Iterator.single({ '?a': 'a', '?c': rdf.DBPEDIA + 'York' }); }

    describe('a GraphPatternIterator for the York query', function () {
      var iterator = new GraphPatternIterator(createSource(),
        yorkQuery, { fragmentsClient: testClient });
      it('should be an iterator of matching ?a/?p/?o bindings', function (done) {
        var yorkBindings = testClient.getBindingsByPattern(patterns.p_birthplace_york)
            .map(function (binding) {
              return { '?a': 'a', '?c': rdf.DBPEDIA + 'York', '?p': binding.subject };
            });
        var expectedBindings = yorkBindings;
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });
});
