/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var DistinctIterator = require('../../lib/iterators/DistinctIterator');

var AsyncIterator = require('asynciterator');

describe('DistinctIterator', function () {
  describe('The DistinctIterator module', function () {
    it('should make DistinctIterator objects', function () {
      DistinctIterator().should.be.an.instanceof(DistinctIterator);
    });

    it('should be a DistinctIterator constructor', function () {
      new DistinctIterator().should.be.an.instanceof(DistinctIterator);
    });

    it('should make AsyncIterator objects', function () {
      DistinctIterator().should.be.an.instanceof(AsyncIterator);
    });

    it('should be an AsyncIterator constructor', function () {
      new DistinctIterator().should.be.an.instanceof(AsyncIterator);
    });
  });

  describe('A DistinctIterator with an empty source', function () {
    var iterator = new DistinctIterator(AsyncIterator.empty());
    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A DistinctIterator', function () {
    describe('with a single item', function () {
      var iterator = new DistinctIterator(AsyncIterator.single(1));
      it('should return the item', function (done) {
        iterator.should.be.an.asyncIteratorOf([1], done);
      });
    });

    describe('with 8 items', function () {
      var iterator = new DistinctIterator(AsyncIterator.fromArray([2, 1, 3, 3, 1, 2, 4, 2]));
      it('should return the unique items', function (done) {
        iterator.should.be.an.asyncIteratorOf([2, 1, 3, 4], done);
      });
    });
  });
});
