/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var TriplePatternIterator = require('../../lib/triple-pattern-fragments/TriplePatternIterator');

var AsyncIterator = require('asynciterator'),
    FileFragmentsClient = require('../lib/FileFragmentsClient'),
    rdf = require('../../lib/util/RdfUtil');

var testClient = new FileFragmentsClient();

var patterns = {
  york_p_o: { subject: rdf.DBPEDIA + 'York', predicate: '?p', object: '?o' },
  s_type_artist: { subject: '?s', predicate: rdf.RDF_TYPE, object: rdf.DBPEDIAOWL + 'Artist' },
  s_birthplace_york: { subject: '?s', predicate: rdf.DBPEDIAOWL + 'birthPlace', object: rdf.DBPEDIA + 'York' },
};

describe('TriplePatternIterator', function () {
  describe('The TriplePatternIterator module', function () {
    it('should be a function', function () {
      TriplePatternIterator.should.be.a('function');
    });

    it('should make TriplePatternIterator objects', function () {
      TriplePatternIterator().should.be.an.instanceof(TriplePatternIterator);
    });

    it('should be a TriplePatternIterator constructor', function () {
      new TriplePatternIterator().should.be.an.instanceof(TriplePatternIterator);
    });

    it('should make AsyncIterator objects', function () {
      TriplePatternIterator().should.be.an.instanceof(AsyncIterator);
    });

    it('should be an AsyncIterator constructor', function () {
      new TriplePatternIterator().should.be.an.instanceof(AsyncIterator);
    });
  });

  describe('a TriplePatternIterator with an empty parent', function () {
    function createSource() { return AsyncIterator.empty(); }

    describe('a TriplePatternIterator for dbpedia:York ?p ?o', function () {
      var iterator = new TriplePatternIterator(createSource(),
        patterns.york_p_o, { fragmentsClient: testClient });
      it('should return no bindings', function (done) {
        var expectedBindings = [];
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });

  describe('a TriplePatternIterator passed an empty binding', function () {
    function createSource() { return AsyncIterator.single({}); }

    describe('a TriplePatternIterator for dbpedia:York ?p ?o', function () {
      var iterator = new TriplePatternIterator(createSource(),
        patterns.york_p_o, { fragmentsClient: testClient });
      it('should be an iterator of ?p/?o bindings', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.york_p_o)
            .map(function (binding) {
              return { '?p': binding.predicate, '?o': binding.object };
            });
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });

  describe('a TriplePatternIterator passed a single non-overlapping bindings object', function () {
    function createSource() { return AsyncIterator.single({ '?a': 'a' }); }

    describe('a TriplePatternIterator for dbpedia:York ?p ?o', function () {
      var iterator = new TriplePatternIterator(createSource(),
        patterns.york_p_o, { fragmentsClient: testClient });
      it('should be an iterator of ?a/?p/?o bindings', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.york_p_o)
            .map(function (binding) {
              return { '?a': 'a', '?p': binding.predicate, '?o': binding.object };
            });
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });

  describe('a TriplePatternIterator passed a single overlapping bindings object', function () {
    function createSource() {
      return AsyncIterator.single({ '?a': 'a', '?p': rdf.RDF_TYPE });
    }

    describe('a TriplePatternIterator for York ?p ?o', function () {
      var iterator = new TriplePatternIterator(createSource(),
        patterns.york_p_o, { fragmentsClient: testClient });
      it('should only return compatible bindings', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.york_p_o)
            .filter(function (binding) { return binding.predicate === rdf.RDF_TYPE; })
            .map(function (binding) {
              return { '?a': 'a', '?p': binding.predicate, '?o': binding.object };
            });
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });

  describe('when passed an iterator for ?s a Artist', function () {
    function createSource() {
      return new TriplePatternIterator(AsyncIterator.single({}),
        patterns.s_type_artist, { fragmentsClient: testClient });
    }

    describe('a TriplePatternIterator for York ?p ?o', function () {
      var iterator = new TriplePatternIterator(createSource(),
        patterns.york_p_o, { fragmentsClient: testClient });
      it('should return the cartesian product', function (done) {
        var artistBindings = testClient.getBindingsByPattern(patterns.s_type_artist);
        var yorkBindings = testClient.getBindingsByPattern(patterns.york_p_o);
        var expectedBindings = [];
        artistBindings.forEach(function (artistBindings) {
          yorkBindings.forEach(function (yorkBindings) {
            expectedBindings.push({
              '?s': artistBindings.subject,
              '?p': yorkBindings.predicate,
              '?o': yorkBindings.object,
            });
          });
        });
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });

    describe('a TriplePatternIterator for ?s birthPlace York', function () {
      var iterator = new TriplePatternIterator(createSource(),
        patterns.s_birthplace_york, { fragmentsClient: testClient });
      it('should return only those triples that match both patterns', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.s_type_artist)
            .filter(function (bindings) { return (/Flaxman|Robson|Tuke/).test(bindings.subject); })
            .map(function (bindings) { return { '?s': bindings.subject }; });
        iterator.should.be.an.iteratorOf(expectedBindings, done);
      });
    });
  });
});
