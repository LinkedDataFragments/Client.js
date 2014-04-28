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

  describe('A SortIterator without source', function () {
    var iterator = new SortIterator();

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A SortIterator having an infinite window length', function () {
    describe('with a single item', function () {
      var iterator = new SortIterator(Iterator.single(1)), endEmitted = 0;
      iterator.on('end', function () { endEmitted++; });

      it('should return the item', function () {
        expect(iterator.read()).to.equal(1);
      });

      it('should not read more items', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should have emitted end', function () {
        endEmitted.should.equal(1);
      });
    });

    describe('with 8 items', function () {
      var iterator = new SortIterator(Iterator.fromArray([8, 6, 4, 3, 1, 2, 7, 5])), endEmitted = 0;
      iterator.on('end', function () { endEmitted++; });

      it('should return the sorted items', function () {
        expect(iterator.read()).to.equal(1);
        expect(iterator.read()).to.equal(2);
        expect(iterator.read()).to.equal(3);
        expect(iterator.read()).to.equal(4);
        expect(iterator.read()).to.equal(5);
        expect(iterator.read()).to.equal(6);
        expect(iterator.read()).to.equal(7);
        expect(iterator.read()).to.equal(8);
      });

      it('should not read more items', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should have emitted end', function () {
        endEmitted.should.equal(1);
      });
    });
  });

  describe('A SortIterator having window length 4', function () {
    describe('with a single item', function () {
      var iterator = new SortIterator(Iterator.single(1), { window: 4 }), endEmitted = 0;
      iterator.on('end', function () { endEmitted++; });

      it('should return the item', function () {
        expect(iterator.read()).to.equal(1);
      });

      it('should not read more items', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should have emitted end', function () {
        endEmitted.should.equal(1);
      });
    });

    describe('with 8 items', function () {
      var iterator = new SortIterator(Iterator.fromArray([8, 6, 4, 3, 1, 2, 7, 5]), { window: 4 }),
          endEmitted = 0;
      iterator.on('end', function () { endEmitted++; });

      it('should return the sorted items', function () {
        expect(iterator.read()).to.equal(3);
        expect(iterator.read()).to.equal(1);
        expect(iterator.read()).to.equal(2);
        expect(iterator.read()).to.equal(4);
        expect(iterator.read()).to.equal(5);
        expect(iterator.read()).to.equal(6);
        expect(iterator.read()).to.equal(7);
        expect(iterator.read()).to.equal(8);
      });

      it('should not read more items', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should have emitted end', function () {
        endEmitted.should.equal(1);
      });
    });
  });

  describe('A SortIterator with a custom sort function', function () {
    function reverseSort(a, b) { return b - a; }

    describe('with a single item', function () {
      var iterator = new SortIterator(Iterator.single(1), reverseSort), endEmitted = 0;
      iterator.on('end', function () { endEmitted++; });

      it('should return the item', function () {
        expect(iterator.read()).to.equal(1);
      });

      it('should not read more items', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should have emitted end', function () {
        endEmitted.should.equal(1);
      });
    });

    describe('with 8 items', function () {
      var iterator = new SortIterator(Iterator.fromArray([8, 6, 4, 3, 1, 2, 7, 5]), reverseSort),
          endEmitted = 0;
      iterator.on('end', function () { endEmitted++; });

      it('should return the sorted items', function () {
        expect(iterator.read()).to.equal(8);
        expect(iterator.read()).to.equal(7);
        expect(iterator.read()).to.equal(6);
        expect(iterator.read()).to.equal(5);
        expect(iterator.read()).to.equal(4);
        expect(iterator.read()).to.equal(3);
        expect(iterator.read()).to.equal(2);
        expect(iterator.read()).to.equal(1);
      });

      it('should not read more items', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should have emitted end', function () {
        endEmitted.should.equal(1);
      });
    });
  });
});
