/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var ReorderingGraphPatternIterator = require('../../lib/triple-pattern-fragments/ReorderingGraphPatternIterator');

var AsyncIterator = require('asynciterator'),
    TriplePatternIterator = require('../../lib/triple-pattern-fragments/TriplePatternIterator'),
    FileFragmentsClient = require('../lib/FileFragmentsClient'),
    rdf = require('../../lib/util/RdfUtil');

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
testClient._metadata = {
  '$-type-artist':  { totalTriples: 500000 },
  '$-birthplace-$': { totalTriples: 1000000 },
  '$-name-york':    { totalTriples: 10 },
};

describe('ReorderingGraphPatternIterator', function () {
  describe('The ReorderingGraphPatternIterator module', function () {
    it('should be a function', function () {
      ReorderingGraphPatternIterator.should.be.a('function');
    });

    it('should make ReorderingGraphPatternIterator objects', function () {
      ReorderingGraphPatternIterator(null, yorkQuery).should.be.an.instanceof(ReorderingGraphPatternIterator);
    });

    it('should be a ReorderingGraphPatternIterator constructor', function () {
      new ReorderingGraphPatternIterator(null, yorkQuery).should.be.an.instanceof(ReorderingGraphPatternIterator);
    });

    it('should make AsyncIterator objects', function () {
      ReorderingGraphPatternIterator(null, yorkQuery).should.be.an.instanceof(AsyncIterator);
    });

    it('should be an AsyncIterator constructor', function () {
      new ReorderingGraphPatternIterator(null, yorkQuery).should.be.an.instanceof(AsyncIterator);
    });
  });

  describe('A ReorderingGraphPatternIterator created with the empty graph', function () {
    var iterator = new ReorderingGraphPatternIterator(null, []);
    it('should be a simple TransformIterator', function () {
      iterator.should.be.an.instanceof(AsyncIterator.TransformIterator);
      iterator.should.not.be.an.instanceof(ReorderingGraphPatternIterator);
    });
  });

  describe('A ReorderingGraphPatternIterator created with a single-element graph', function () {
    var triple = rdf.triple('?a', 'b', 'c'),
        iterator = new ReorderingGraphPatternIterator(null, [triple]);
    it('should be a triple pattern iterator', function () {
      iterator.should.be.an.instanceof(TriplePatternIterator);
      iterator.should.not.be.an.instanceof(ReorderingGraphPatternIterator);
    });
    it('should iterate over that triple pattern', function () {
      iterator.should.have.property('_pattern', triple);
    });
  });

  describe('A ReorderingGraphPatternIterator with an empty parent', function () {
    function createSource() { return AsyncIterator.empty(); }

    describe('passed a ReorderingGraphPatternIterator for the York query', function () {
      var iterator = new ReorderingGraphPatternIterator(createSource(),
        yorkQuery, { fragmentsClient: testClient });
      it('should return no bindings', function (done) {
        var expectedBindings = [];
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });

  describe('A ReorderingGraphPatternIterator passed a single non-overlapping bindings object', function () {
    function createSource() { return AsyncIterator.single({ '?a': 'a' }); }

    describe('passed a ReorderingGraphPatternIterator for the York query', function () {
      var iterator = new ReorderingGraphPatternIterator(createSource(),
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
    function createSource() { return AsyncIterator.single({ '?a': 'a', '?c': rdf.DBPEDIA + 'York' }); }

    describe('a ReorderingGraphPatternIterator for the York query', function () {
      var iterator = new ReorderingGraphPatternIterator(createSource(),
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
