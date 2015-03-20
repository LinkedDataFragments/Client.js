/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var TurtleFragmentIterator = require('../../lib/triple-pattern-fragments/TurtleFragmentIterator');
var Iterator = require('../../lib/iterators/Iterator'),
    fs = require('fs');

describe('TurtleFragmentIterator', function () {
  describe('The TurtleFragmentIterator module', function () {
    it('should be a function', function () {
      TurtleFragmentIterator.should.be.a('function');
    });

    it('should make TurtleFragmentIterator objects', function () {
      TurtleFragmentIterator().should.be.an.instanceof(TurtleFragmentIterator);
    });

    it('should be a TurtleFragmentIterator constructor', function () {
      new TurtleFragmentIterator().should.be.an.instanceof(TurtleFragmentIterator);
    });

    it('should make Iterator objects', function () {
      TurtleFragmentIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new TurtleFragmentIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('The TurtleFragmentIterator class', function () {
    it('should support Turtle', function () {
      TurtleFragmentIterator.supportsContentType('text/turtle').should.be.true;
    });

    it('should support Notation3', function () {
      TurtleFragmentIterator.supportsContentType('text/n3').should.be.true;
    });

    it('should support N-Triples', function () {
      TurtleFragmentIterator.supportsContentType('application/n-triples').should.be.true;
    });

    it('should not support TriG', function () {
      TurtleFragmentIterator.supportsContentType('application/trig').should.be.false;
    });

    it('should not support N-Quads', function () {
      TurtleFragmentIterator.supportsContentType('application/n-quads').should.be.false;
    });
  });

  describe('A TurtleFragmentIterator for a fragment', function () {
    var fragment;
    before(function () {
      var source = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl');
      fragment = new TurtleFragmentIterator(source, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
    });

    it('should return all data triples in the fragment', function (done) {
      fragment.should.be.an.iteratorWithLength(22, done);
    });

    it('should return fragment metadata in the metadata stream', function (done) {
      fragment.metadataStream.should.be.an.iteratorWithLength(22, done);
    });
  });

  describe('A TurtleFragmentIterator for a fragment that is not read', function () {
    var fragment;
    before(function () {
      var source = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl');
      fragment = new TurtleFragmentIterator(source, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
    });

    it('should return fragment metadata in the metadata stream', function (done) {
      fragment.metadataStream.should.be.an.iteratorWithLength(22, done);
    });
  });
});
