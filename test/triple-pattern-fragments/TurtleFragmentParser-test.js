/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var TurtleFragmentParser = require('../../lib/triple-pattern-fragments/TurtleFragmentParser');
var Iterator = require('../../lib/iterators/Iterator'),
    fs = require('fs');

describe('TurtleFragmentParser', function () {
  describe('The TurtleFragmentParser module', function () {
    it('should be a function', function () {
      TurtleFragmentParser.should.be.a('function');
    });

    it('should make TurtleFragmentParser objects', function () {
      TurtleFragmentParser().should.be.an.instanceof(TurtleFragmentParser);
    });

    it('should be a TurtleFragmentParser constructor', function () {
      new TurtleFragmentParser().should.be.an.instanceof(TurtleFragmentParser);
    });

    it('should make Iterator objects', function () {
      TurtleFragmentParser().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new TurtleFragmentParser().should.be.an.instanceof(Iterator);
    });
  });

  describe('The TurtleFragmentParser class', function () {
    it('should support Turtle', function () {
      TurtleFragmentParser.supportsContentType('text/turtle').should.be.true;
    });

    it('should support Notation3', function () {
      TurtleFragmentParser.supportsContentType('text/n3').should.be.true;
    });

    it('should support N-Triples', function () {
      TurtleFragmentParser.supportsContentType('application/n-triples').should.be.true;
    });

    it('should not support HTML', function () {
      TurtleFragmentParser.supportsContentType('text/html').should.be.false;
    });
  });

  describe('A TurtleFragmentParser for a fragment', function () {
    var fragment;
    before(function () {
      var source = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl');
      fragment = new TurtleFragmentParser(source, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
    });

    it('should return all data triples in the fragment', function (done) {
      fragment.should.be.an.iteratorWithLength(27, done);
    });

    it('should return fragment metadata in the metadata stream', function (done) {
      fragment.metadataStream.should.be.an.iteratorWithLength(17, done);
    });
  });

  describe('A TurtleFragmentParser for a fragment that is not read', function () {
    var fragment;
    before(function () {
      var source = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl');
      fragment = new TurtleFragmentParser(source, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
    });

    it('should return fragment metadata in the metadata stream', function (done) {
      fragment.metadataStream.should.be.an.iteratorWithLength(17, done);
    });
  });
});
