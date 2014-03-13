/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var EmptyIterator = require('../../lib/iterators/EmptyIterator');
var Stream = require('stream').Stream;

describe('EmptyIterator', function () {
  describe('The EmptyIterator module', function () {
    it('should be a function', function () {
      EmptyIterator.should.be.a('function');
    });

    it('should make EmptyIterator objects', function () {
      EmptyIterator().should.be.an.instanceof(EmptyIterator);
    });

    it('should be a EmptyIterator constructor', function () {
      new EmptyIterator().should.be.an.instanceof(EmptyIterator);
    });

    it('should make Stream objects', function () {
      EmptyIterator().should.be.an.instanceof(Stream);
    });

    it('should be a Stream constructor', function () {
      new EmptyIterator().should.be.an.instanceof(Stream);
    });
  });

  describe('An EmptyIterator', function () {
    var iterator = new EmptyIterator();
    it('should be an empty stream', function (done) {
      iterator.should.be.a.streamOf([], done);
    });
  });
});
