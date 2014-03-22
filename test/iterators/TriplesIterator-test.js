/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var TriplesIterator = require('../../lib/iterators/TriplesIterator');

var Iterator = require('../../lib/iterators/Iterator'),
    Readable = require('stream').Readable,
    fs = require('fs'),
    rdf = require('../../lib/rdf/RdfUtil');

describe('TriplesIterator', function () {
  describe('The TriplesIterator module', function () {
    it('should make TriplesIterator objects', function () {
      TriplesIterator().should.be.an.instanceof(TriplesIterator);
    });

    it('should be a TriplesIterator constructor', function () {
      new TriplesIterator().should.be.an.instanceof(TriplesIterator);
    });

    it('should make Iterator objects', function () {
      TriplesIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new TriplesIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('An TriplesIterator instance without parameters', function () {
    var iterator = new TriplesIterator();
    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('An TriplesIterator instance with an empty iterator', function () {
    var iterator = new TriplesIterator(Iterator.empty());
    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('An TriplesIterator instance with a single triple', function () {
    var iterator = new TriplesIterator(Iterator.single('<a> <b> <c>.'));

    it('should parse the triple', function () {
      expect(iterator.read()).to.be.a.triple('a', 'b', 'c');
    });

    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('An TriplesIterator instance parsing a file', function () {
    var iterator;
    var triples = [];
    before(function (done) {
      var stream = fs.createReadStream(__dirname + '/../data/fragments/$-birthplace-york.ttl');
      iterator = new TriplesIterator(stream);
      iterator.on('readable', function () {
        var triple;
        while (triple = iterator.read())
          triples.push(triple);
      });
      iterator.on('end', done);
    });

    it('should parse the triple', function () {
      expect(triples).to.have.length(39);
    });

    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });
});
