/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var WindowTransformIterator = require('../../lib/iterators/WindowTransformIterator');

var Iterator = require('../../lib/iterators/Iterator');

describe('WindowTransformIterator', function () {
  describe('The WindowTransformIterator module', function () {
    it('should make WindowTransformIterator objects', function () {
      WindowTransformIterator().should.be.an.instanceof(WindowTransformIterator);
    });

    it('should be a WindowTransformIterator constructor', function () {
      new WindowTransformIterator().should.be.an.instanceof(WindowTransformIterator);
    });

    it('should make Iterator objects', function () {
      WindowTransformIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new WindowTransformIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('A WindowTransformIterator without source', function () {
    var iterator = new WindowTransformIterator();

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A WindowTransformIterator without _transformWindow implementation', function () {
    var iterator = new WindowTransformIterator(Iterator.single(1));

    it('should throw an error', function () {
      (function () { iterator.read(); })
        .should.throw('The _transformWindow method has not been implemented.');
    });
  });

  describe('A WindowTransformIterator having an infinite window length', function () {
    describe('with a single item and two tranformed items per item', function () {
      var iterator = new WindowTransformIterator(Iterator.single(1)), endEmitted = 0;
      iterator._transformWindow = function (items, done) {
        items.should.deep.equal([1]);
        this._push(3);
        this._push(4);
        done();
      };
      iterator.on('end', function () { endEmitted++; });

      it('should return the first transformed item', function () {
        expect(iterator.read()).to.equal(3);
      });

      it('should return the second transformed item', function () {
        expect(iterator.read()).to.equal(4);
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

    describe('with 8 items and two tranformed items per item', function () {
      var iterator = new WindowTransformIterator(Iterator.fromArray([1, 2, 3, 4, 5, 6, 7, 8])),
          endEmitted = 0;
      iterator._transformWindow = function (items, done) {
        items.should.deep.equal([1, 2, 3, 4, 5, 6, 7, 8]);
        this._push('a');
        this._push('b');
        done();
        done(); // multiple done calls should be ignored
      };
      iterator.on('end', function () { endEmitted++; });

      it('should return the first transformed item', function () {
        expect(iterator.read()).to.equal('a');
      });

      it('should return the second transformed item', function () {
        expect(iterator.read()).to.equal('b');
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

  describe('A WindowTransformIterator having window length 4', function () {
    describe('with a single item and two tranformed items per item', function () {
      var iterator = new WindowTransformIterator(Iterator.single(1), { window: 4 }),
          endEmitted = 0;
      iterator._transformWindow = function (items, done) {
        items.should.deep.equal([1]);
        this._push(3);
        this._push(4);
        done();
        done(); // multiple done calls should be ignored
      };
      iterator.on('end', function () { endEmitted++; });

      it('should return the first transformed item', function () {
        expect(iterator.read()).to.equal(3);
      });

      it('should return the second transformed item', function () {
        expect(iterator.read()).to.equal(4);
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

    describe('with 8 items and two tranformed items per item', function () {
      var iterator = new WindowTransformIterator(
                          Iterator.fromArray([1, 2, 3, 4, 5, 6, 7, 8]), { window: 4 }),
          endEmitted = 0;
      iterator._transformWindow = function (items, done) {
        this._push(Math.min.apply(null, items));
        this._push(Math.max.apply(null, items));
        done(items.slice(1));
        done([1]); // multiple done calls should be ignored
      };
      iterator.on('end', function () { endEmitted++; });

      it('should return the transformed items', function () {
        expect(iterator.read()).to.equal(1); // min of 1-4
        expect(iterator.read()).to.equal(4); // max of 1-4
        expect(iterator.read()).to.equal(2); // min of 2-5
        expect(iterator.read()).to.equal(5); // max of 2-5
        expect(iterator.read()).to.equal(3); // min of 3-6
        expect(iterator.read()).to.equal(6); // max of 3-6
        expect(iterator.read()).to.equal(4); // min of 4-7
        expect(iterator.read()).to.equal(7); // max of 4-7
        expect(iterator.read()).to.equal(5); // min of 5-8
        expect(iterator.read()).to.equal(8); // max of 5-8
        expect(iterator.read()).to.equal(6); // min of 6-8
        expect(iterator.read()).to.equal(8); // max of 8-8
        expect(iterator.read()).to.equal(7); // min of 7-8
        expect(iterator.read()).to.equal(8); // max of 8-8
        expect(iterator.read()).to.equal(8); // min of 7-8
        expect(iterator.read()).to.equal(8); // max of 8-8
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
