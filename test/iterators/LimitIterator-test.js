/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var LimitIterator = require('../../lib/iterators/LimitIterator');

var Iterator = require('../../lib/iterators/Iterator');

describe('LimitIterator', function () {
  describe('The LimitIterator module', function () {
    it('should make LimitIterator objects', function () {
      LimitIterator().should.be.an.instanceof(LimitIterator);
    });

    it('should be a LimitIterator constructor', function () {
      new LimitIterator().should.be.an.instanceof(LimitIterator);
    });

    it('should make Iterator objects', function () {
      LimitIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new LimitIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('A LimitIterator with an empty source', function () {
    var iterator = new LimitIterator(Iterator.empty());

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A LimitIterator with limit 0', function () {
    var iterator = new LimitIterator(Iterator.single(1), 0, 0);

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A LimitIterator with a negative limit', function () {
    var iterator = new LimitIterator(Iterator.single(1), 0, -5);

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A LimitIterator without offset and limit', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]));

    it('should return all items', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.deep.equal([1, 2, 3, 4, 5, 6]);
        done();
      });
    });
  });

  describe('A LimitIterator with offset 0 and limit Infinity', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]), 0, Infinity);

    it('should return all items', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.deep.equal([1, 2, 3, 4, 5, 6]);
        done();
      });
    });
  });


  describe('A LimitIterator with offset 0 and limit 2', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]), 0, 2);

    it('should return the first 2 items', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.deep.equal([1, 2]);
        done();
      });
    });
  });

  describe('A LimitIterator with offset 2 and limit Infinity', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]), 2, Infinity);

    it('should return all items except the first 2', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.deep.equal([3, 4, 5, 6]);
        done();
      });
    });
  });

  describe('A LimitIterator with offset 2 and limit 3', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]), 2, 3);

    it('should return three items after the second', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.deep.equal([3, 4, 5]);
        done();
      });
    });
  });

  describe('A LimitIterator with offset larger than the source size', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]), 10, Infinity);

    it('should not return any items', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.have.length(0);
        done();
      });
    });
  });

  describe('A LimitIterator with offset 0 and limit larger than the source size', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]), 0, 10);

    it('should return all items', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.deep.equal([1, 2, 3, 4, 5, 6]);
        done();
      });
    });
  });

  describe('A LimitIterator with offset and limit larger than the source size', function () {
    var iterator = new LimitIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6]), 10, 20);

    it('should not return any items', function (done) {
      iterator.toArray(function (error, items) {
        expect(error).to.be.undefined;
        items.should.have.length(0);
        done();
      });
    });
  });
});
