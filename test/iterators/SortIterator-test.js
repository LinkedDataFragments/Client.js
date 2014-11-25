/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var SortIterator = require('../../lib/iterators/SortIterator');

var Iterator = require('../../lib/iterators/Iterator');

describe('SortIterator', function () {
  describe('The SortIterator module', function () {
    it('should make SortIterator objects', function () {
      SortIterator().should.be.an.instanceof(SortIterator);
    });

    it('should be a SortIterator constructor', function () {
      new SortIterator().should.be.an.instanceof(SortIterator);
    });

    it('should make Iterator objects', function () {
      SortIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new SortIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('A SortIterator with an empty source', function () {
    var iterator = new SortIterator(Iterator.empty());

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A SortIterator having an infinite window length', function () {
    describe('with a single item', function () {
      var iterator = new SortIterator(Iterator.single(1));
      it('should return the item', function (done) {
        iterator.should.be.an.iteratorOf([1], done);
      });
    });

    describe('with 8 items', function () {
      var iterator = new SortIterator(Iterator.fromArray([8, 6, 4, 3, 1, 2, 7, 5]));
      it('should return the sorted items', function (done) {
        iterator.should.be.an.iteratorOf([1, 2, 3, 4, 5, 6, 7, 8], done);
      });
    });
  });

  describe('A SortIterator having window length 4', function () {
    describe('with a single item', function () {
      var iterator = new SortIterator(Iterator.single(1), { window: 4 });
      it('should return the item', function (done) {
        iterator.should.be.an.iteratorOf([1], done);
      });
    });

    describe('with 8 items', function () {
      var iterator = new SortIterator(Iterator.fromArray([8, 6, 4, 3, 1, 2, 7, 5]), { window: 4 });
      it('should return the items, sorted with a lookahead of 4', function (done) {
        iterator.should.be.an.iteratorOf([3, 1, 2, 4, 5, 6, 7, 8], done);
      });
    });
  });

  describe('A SortIterator with a custom sort function', function () {
    function reverseSort(a, b) { return b - a; }

    describe('with a single item', function () {
      var iterator = new SortIterator(Iterator.single(1), reverseSort);
      it('should return the item', function (done) {
        iterator.should.be.an.iteratorOf([1], done);
      });
    });

    describe('with 8 items', function () {
      var iterator = new SortIterator(Iterator.fromArray([8, 6, 4, 3, 1, 2, 7, 5]), reverseSort);
      it('should return the sorted items', function (done) {
        iterator.should.be.an.iteratorOf([8, 7, 6, 5, 4, 3, 2, 1], done);
      });
    });
  });
});
