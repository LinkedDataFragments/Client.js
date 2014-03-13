/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var SingleBindingsIterator = require('../../lib/iterators/SingleBindingsIterator');
var Stream = require('stream').Stream;

describe('SingleBindingsIterator', function () {
  describe('The SingleBindingsIterator module', function () {
    it('should be a function', function () {
      SingleBindingsIterator.should.be.a('function');
    });

    it('should make SingleBindingsIterator objects', function () {
      SingleBindingsIterator().should.be.an.instanceof(SingleBindingsIterator);
    });

    it('should be a SingleBindingsIterator constructor', function () {
      new SingleBindingsIterator().should.be.an.instanceof(SingleBindingsIterator);
    });

    it('should make Stream objects', function () {
      SingleBindingsIterator().should.be.an.instanceof(Stream);
    });

    it('should be a Stream constructor', function () {
      new SingleBindingsIterator().should.be.an.instanceof(Stream);
    });
  });

  describe('A SingleBindingsIterator without arguments', function () {
    var iterator = new SingleBindingsIterator();
    it('should stream an empty bindings object', function (done) {
      iterator.should.be.a.streamOf([{ bindings: {} }], done);
    });
  });

  describe('A SingleBindingsIterator with a falsy argument', function () {
    var iterator = new SingleBindingsIterator(null);
    it('should stream an empty bindings object', function (done) {
      iterator.should.be.a.streamOf([{ bindings: {} }], done);
    });
  });

  describe('A SingleBindingsIterator with a bindings object', function () {
    var bindings = { '?a': 'a', '?b': 'b' };
    var iterator = new SingleBindingsIterator(bindings);
    bindings.c = '?c';
    it('should stream a copy of the bindings object', function (done) {
      iterator.should.be.a.streamOf([{ bindings: { '?a': 'a', '?b': 'b' } }], done);
    });
  });
});
