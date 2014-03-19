/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var SingleItemIterator = require('../../lib/iterators/SingleItemIterator');
var Stream = require('stream').Stream;

describe('SingleItemIterator', function () {
  describe('The SingleItemIterator module', function () {
    it('should be a function', function () {
      SingleItemIterator.should.be.a('function');
    });

    it('should make SingleItemIterator objects', function () {
      SingleItemIterator().should.be.an.instanceof(SingleItemIterator);
    });

    it('should be a SingleItemIterator constructor', function () {
      new SingleItemIterator().should.be.an.instanceof(SingleItemIterator);
    });

    it('should make Stream objects', function () {
      SingleItemIterator().should.be.an.instanceof(Stream);
    });

    it('should be a Stream constructor', function () {
      new SingleItemIterator().should.be.an.instanceof(Stream);
    });
  });

  describe('A SingleItemIterator without arguments', function () {
    var iterator = new SingleItemIterator();
    it('should stream an empty bindings object', function (done) {
      iterator.should.be.a.streamOf([{ bindings: {} }], done);
    });
  });

  describe('A SingleItemIterator with a falsy argument', function () {
    var iterator = new SingleItemIterator(null);
    it('should stream an empty bindings object', function (done) {
      iterator.should.be.a.streamOf([{ bindings: {} }], done);
    });
  });

  describe('A SingleItemIterator with a bindings object', function () {
    var bindings = { '?a': 'a', '?b': 'b' };
    var iterator = new SingleItemIterator(bindings);
    bindings.c = '?c';
    it('should stream a copy of the bindings object', function (done) {
      iterator.should.be.a.streamOf([{ bindings: { '?a': 'a', '?b': 'b' } }], done);
    });
  });
});
