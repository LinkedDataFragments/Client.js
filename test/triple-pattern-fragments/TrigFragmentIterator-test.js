/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var TrigFragmentIterator = require('../../lib/triple-pattern-fragments/TrigFragmentIterator');
var Iterator = require('../../lib/iterators/Iterator'),
    fs = require('fs');

describe('TrigFragmentIterator', function () {
  describe('The TrigFragmentIterator module', function () {
    it('should be a function', function () {
      TrigFragmentIterator.should.be.a('function');
    });

    it('should make TrigFragmentIterator objects', function () {
      TrigFragmentIterator().should.be.an.instanceof(TrigFragmentIterator);
    });

    it('should be a TrigFragmentIterator constructor', function () {
      new TrigFragmentIterator().should.be.an.instanceof(TrigFragmentIterator);
    });

    it('should make Iterator objects', function () {
      TrigFragmentIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new TrigFragmentIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('The TrigFragmentIterator class', function () {
    it('should not support Turtle', function () {
      TrigFragmentIterator.supportsContentType('text/turtle').should.be.false;
    });

    it('should not support Notation3', function () {
      TrigFragmentIterator.supportsContentType('text/n3').should.be.false;
    });

    it('should not support N-Triples', function () {
      TrigFragmentIterator.supportsContentType('application/n-triples').should.be.false;
    });

    it('should support TriG', function () {
      TrigFragmentIterator.supportsContentType('application/trig').should.be.true;
    });

    it('should support N-Quads', function () {
      TrigFragmentIterator.supportsContentType('application/n-quads').should.be.true;
    });
  });

  describe('A TrigFragmentIterator for a fragment', function () {
    var fragment;
    before(function () {
      var source = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.trig');
      fragment = new TrigFragmentIterator(source, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
    });

    it('should return all data triples in the fragment', function (done) {
      fragment.should.be.an.iteratorWithLength(10, done);
    });

    it('should return fragment metadata in the metadata stream', function (done) {
      fragment.metadataStream.should.be.an.iteratorWithLength(34, done);
    });
  });

  describe('A TrigFragmentIterator for a fragment that is not read', function () {
    var fragment;
    before(function () {
      var source = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.trig');
      fragment = new TrigFragmentIterator(source, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
    });

    it('should return fragment metadata in the metadata stream', function (done) {
      fragment.metadataStream.should.be.an.iteratorWithLength(34, done);
    });
  });
});
