/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var FilterIterator = require('../../lib/iterators/FilterIterator');

var Iterator = require('../../lib/iterators/Iterator');

describe('FilterIterator', function () {
  describe('The FilterIterator module', function () {
    it('should make FilterIterator objects', function () {
      FilterIterator().should.be.an.instanceof(FilterIterator);
    });

    it('should be a FilterIterator constructor', function () {
      new FilterIterator().should.be.an.instanceof(FilterIterator);
    });

    it('should make Iterator objects', function () {
      FilterIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new FilterIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('A FilterIterator with an empty source', function () {
    var iterator;
    before(function () {
      iterator = new FilterIterator(Iterator.empty());
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A FilterIterator without _filter implementation', function () {
    var iterator;
    before(function () {
      iterator = new FilterIterator(Iterator.single(1));
    });

    it('should throw an error', function () {
      (function () { iterator.read(); })
        .should.throw('The _filter method has not been implemented.');
    });
  });

  describe('A FilterIterator with a single item that is matched by the filter', function () {
    var iterator, endEmitted = 0;
    before(function () {
      iterator = new FilterIterator(Iterator.single(1),
                                    function (item) { return item === 1; });
      iterator.on('end', function () { endEmitted++; });
    });

    it('should return the item', function () {
      expect(iterator.read()).to.equal(1);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });

    it('should have emitted end', function () {
      endEmitted.should.equal(1);
    });
  });

  describe('A FilterIterator with a single item that is not matched by the filter', function () {
    var iterator, endEmitted = 0;
    before(function () {
      iterator = new FilterIterator(Iterator.single(1),
                                    function (item) { return item === 2; });
      iterator.on('end', function () { endEmitted++; });
    });

    it('should return the item', function () {
      expect(iterator.read()).to.equal(null);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });

    it('should have emitted end', function () {
      endEmitted.should.equal(1);
    });
  });

  describe('A FilterIterator with multiple items and a filter', function () {
    var iterator, endEmitted = 0;
    before(function () {
      iterator = new FilterIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]),
                                    function (item) { return item % 2; });
      iterator.on('end', function () { endEmitted++; });
    });

    it('should return the items that match the filter', function () {
      expect(iterator.read()).to.equal(1);
      expect(iterator.read()).to.equal(3);
      expect(iterator.read()).to.equal(5);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });

    it('should have emitted end', function () {
      endEmitted.should.equal(1);
    });
  });
});
