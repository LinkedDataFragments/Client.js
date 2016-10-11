/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var CountExtractor = require('../../lib/extractors/CountExtractor');

var AsyncIterator = require('asynciterator'),
    rdf = require('../../lib/util/RdfUtil');

describe('CountExtractor', function () {
  describe('The CountExtractor module', function () {
    it('should be a function', function () {
      CountExtractor.should.be.a('function');
    });

    it('should make CountExtractor objects', function () {
      CountExtractor().should.be.an.instanceof(CountExtractor);
    });

    it('should be an CountExtractor constructor', function () {
      new CountExtractor().should.be.an.instanceof(CountExtractor);
    });
  });

  describe('A CountExtractor instance without options', function () {
    var countExtractor = new CountExtractor();

    describe('extracting from an empty stream', function () {
      var metadata;
      before(function (done) {
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, AsyncIterator.empty(),
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit an empty metadata object', function () {
        metadata.should.deep.equal({});
      });
    });

    describe('extracting from a stream without count information', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherPropertyA', '"5678"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit an empty metadata object', function () {
        metadata.should.deep.equal({});
      });
    });

    describe('extracting from a stream with a void:triples count', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherPropertyA', '"5678"'),
          rdf.triple('http://example.org/fragment', rdf.VOID_TRIPLES, '"1234"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit a metadata object with count', function () {
        metadata.should.deep.equal({ totalTriples: 1234 });
      });
    });

    describe('extracting from a stream with a hydra:totalItems count', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherPropertyA',     '"5678"'),
          rdf.triple('http://example.org/fragment', rdf.HYDRA_TOTALITEMS, '"1234"'),
          rdf.triple('http://example.org/fragment', 'otherPropertyB',     '"5678"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit a metadata object with count', function () {
        metadata.should.deep.equal({ totalTriples: 1234 });
      });
    });

    describe('extracting from a stream with multiple count triples', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', rdf.VOID_TRIPLES,     '"1234"'),
          rdf.triple('http://example.org/fragment', rdf.HYDRA_TOTALITEMS, '"5678"'),
          rdf.triple('http://example.org/fragment', rdf.VOID_TRIPLES,     '"5678"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit a metadata object with the first count', function () {
        metadata.should.deep.equal({ totalTriples: 1234 });
      });
    });

    describe('extracting from a stream with counts for different fragments', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragmentA', rdf.VOID_TRIPLES,     '"5678"'),
          rdf.triple('http://example.org/fragmentB', rdf.HYDRA_TOTALITEMS, '"5678"'),
          rdf.triple('http://example.org/fragment',  rdf.VOID_TRIPLES,     '"1234"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit a metadata object with count for the correct fragment', function () {
        metadata.should.deep.equal({ totalTriples: 1234 });
      });
    });

    describe('extracting from a stream without callback', function () {
      var countExtractor, iterator;
      before(function () {
        countExtractor = new CountExtractor();
        iterator = new AsyncIterator();
        countExtractor.extract(null, iterator);
      });

      it('should not throw an error when the stream ends', function () {
        iterator.close();
      });
    });
  });

  describe('A CountExtractor instance with custom count predicates', function () {
    var countExtractor = new CountExtractor({
      countPredicates: ['http://ex.org/countA', 'http://ex.org/countB'],
    });

    describe('extracting from a stream with a void:triples count', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherPropertyA', '"5678"'),
          rdf.triple('http://example.org/fragment', rdf.VOID_TRIPLES, '"1234"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit an empty metadata object', function () {
        metadata.should.deep.equal({});
      });
    });

    describe('extracting from a stream with a hydra:totalItems count', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherPropertyA',     '"5678"'),
          rdf.triple('http://example.org/fragment', rdf.HYDRA_TOTALITEMS, '"1234"'),
          rdf.triple('http://example.org/fragment', 'otherPropertyB',     '"5678"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit an empty metadata object', function () {
        metadata.should.deep.equal({});
      });
    });

    describe('extracting from a stream with a http://ex.org/countA count', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherPropertyA',       '"5678"'),
          rdf.triple('http://example.org/fragment', 'http://ex.org/countA', '"1234"'),
          rdf.triple('http://example.org/fragment', 'otherPropertyB',       '"5678"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit a metadata object with count', function () {
        metadata.should.deep.equal({ totalTriples: 1234 });
      });
    });

    describe('extracting from a stream with a http://ex.org/countB count', function () {
      var metadata;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherPropertyA',       '"5678"'),
          rdf.triple('http://example.org/fragment', 'http://ex.org/countB', '"1234"'),
          rdf.triple('http://example.org/fragment', 'otherPropertyB',       '"5678"'),
        ]);
        countExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                               function (error, m) { metadata = m; done(error); });
      });

      it('should emit a metadata object with count', function () {
        metadata.should.deep.equal({ totalTriples: 1234 });
      });
    });
  });
});
