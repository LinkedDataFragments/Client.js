/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var TurtleFragmentParser = require('../../lib/fragments/TurtleFragmentParser');
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
    var fragment = fs.createReadStream(__dirname + '/../data/fragments/$-birthplace-york.ttl');
    var parser = new TurtleFragmentParser(fragment, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork');

    it('should return all triples in the fragment', function (done) {
      parser.should.be.an.iteratorWithLength(39, done);
    });

    it('should give access to fragment metadata', function (done) {
      parser.getProperty('metadata', function (metadata) {
        metadata.should.deep.equal({ totalTriples: 169 });
        done();
      });
    });
  });
});
