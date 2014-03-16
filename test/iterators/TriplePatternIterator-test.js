/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var TriplePatternIterator = require('../../lib/iterators/TriplePatternIterator');
var Stream = require('stream').Stream,
    EmptyIterator = require('../../lib/iterators/EmptyIterator'),
    SingleBindingsIterator = require('../../lib/iterators/SingleBindingsIterator'),
    FileFragmentsClient = require('../lib/FileFragmentsClient'),
    rdf = require('../../lib/rdf/RdfUtil'),
    _ = require('lodash');

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

    it('should make Stream objects', function () {
      TriplePatternIterator().should.be.an.instanceof(Stream);
    });

    it('should be a Stream constructor', function () {
      new TriplePatternIterator().should.be.an.instanceof(Stream);
    });
  });

  describe('when piped no bindings', function () {
    function createSource() { return EmptyIterator(); }

    describe('a TriplePatternIterator for dbpedia:York ?p ?o', function () {
      var iterator = new TriplePatternIterator(patterns.york_p_o, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should return no bindings', function (done) {
        var expectedBindings = [];
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });

  describe('when piped a single empty bindings object', function () {
    function createSource() { return SingleBindingsIterator({}); }

    describe('a TriplePatternIterator for dbpedia:York ?p ?o', function () {
      var iterator = new TriplePatternIterator(patterns.york_p_o, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should be a stream of ?p/?o bindings', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.york_p_o)
            .map(function (binding) {
              return { bindings: { '?p': binding.predicate, '?o': binding.object } };
            });
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });

  describe('when piped a single non-overlapping bindings object', function () {
    function createSource() { return SingleBindingsIterator({ '?a': 'a' }); }

    describe('a TriplePatternIterator for dbpedia:York ?p ?o', function () {
      var iterator = new TriplePatternIterator(patterns.york_p_o, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should be a stream of ?a/?p/?o bindings', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.york_p_o)
            .map(function (binding) {
              return { bindings: { '?a': 'a', '?p': binding.predicate, '?o': binding.object } };
            });
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });

  describe('when piped a single overlapping bindings object', function () {
    function createSource() {
      return SingleBindingsIterator({ '?a': 'a', '?p': rdf.RDF_TYPE });
    }

    describe('a TriplePatternIterator for York ?p ?o', function () {
      var iterator = new TriplePatternIterator(patterns.york_p_o, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should only return compatible bindings', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.york_p_o)
            .filter(function (binding) { return binding.predicate === rdf.RDF_TYPE; })
            .map(function (binding) {
              return { bindings: { '?a': 'a', '?p': binding.predicate, '?o': binding.object } };
            });
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });

  describe('when piped an iterator for ?s a Artist', function () {
    function createSource() {
      var source = new TriplePatternIterator(patterns.s_type_artist, { fragmentsClient: testClient });
      SingleBindingsIterator().pipe(source);
      return source;
    }

    describe('a TriplePatternIterator for York ?p ?o', function () {
      var iterator = new TriplePatternIterator(patterns.york_p_o, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should return the cartesian product', function (done) {
        var artistBindings = testClient.getBindingsByPattern(patterns.s_type_artist);
        var yorkBindings = testClient.getBindingsByPattern(patterns.york_p_o);
        var expectedBindings = [];
        artistBindings.forEach(function (artistBindings) {
          yorkBindings.forEach(function (yorkBindings) {
            expectedBindings.push({ bindings: {
              '?s': artistBindings.subject,
              '?p': yorkBindings.predicate,
              '?o': yorkBindings.object,
            }});
          });
        });
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });

    describe('a TriplePatternIterator for ?s birthPlace York', function () {
      var iterator = new TriplePatternIterator(patterns.s_birthplace_york, { fragmentsClient: testClient });
      createSource().pipe(iterator);
      it('should return only those triples that match both patterns', function (done) {
        var expectedBindings = testClient.getBindingsByPattern(patterns.s_type_artist)
            .filter(function (bindings) { return (/Flaxman|Robson|Tuke/).test(bindings.subject); })
            .map(function (bindings) { return { bindings: { '?s': bindings.subject } }; });
        iterator.should.be.a.streamOf(expectedBindings, done);
      });
    });
  });
});
