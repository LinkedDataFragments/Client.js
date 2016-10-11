/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var UnionIterator = require('../../lib/sparql/UnionIterator');

var AsyncIterator = require('asynciterator');

describe('UnionIterator', function () {
  describe('The UnionIterator module', function () {
    it('should make UnionIterator objects', function () {
      UnionIterator().should.be.an.instanceof(UnionIterator);
    });

    it('should be a UnionIterator constructor', function () {
      new UnionIterator().should.be.an.instanceof(UnionIterator);
    });

    it('should make AsyncIterator objects', function () {
      UnionIterator().should.be.an.instanceof(AsyncIterator);
    });

    it('should be an AsyncIterator constructor', function () {
      new UnionIterator().should.be.an.instanceof(AsyncIterator);
    });
  });

  describe('A UnionIterator without iterators', function () {
    var iterator;
    before(function () {
      iterator = new UnionIterator();
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A UnionIterator without an empty iterator set', function () {
    var iterator;
    before(function () {
      iterator = new UnionIterator([]);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A UnionIterator with an empty source iterator', function () {
    var iterator;
    before(function () {
      iterator = new UnionIterator([AsyncIterator.empty()]);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A UnionIterator with a null source iterator', function () {
    var iterator;
    before(function () {
      iterator = new UnionIterator([null]);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A UnionIterator with a single source iterator', function () {
    var iterator, source;
    before(function () {
      source = new AsyncIterator.BufferedIterator();
      source._push(1);
      iterator = new UnionIterator([source]);
    });

    describe('after the iterator has been created', function () {
      it('should read the first element', function () {
        expect(iterator.read()).to.equal(1);
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });

    describe('after the source has new items', function () {
      before(function (done) {
        iterator.once('readable', done);
        source._push(2);
        source._push(3);
      });

      it('should read the second element', function () {
        expect(iterator.read()).to.equal(2);
      });

      it('should read the third element', function () {
        expect(iterator.read()).to.equal(3);
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });

    describe('after the source has ended', function () {
      before(function (done) {
        iterator.on('end', done);
        source.close();
      });

      it('should should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });
  });

  describe('A UnionIterator with two source iterators', function () {
    var iterator, sourceA, sourceB;
    before(function () {
      sourceA = new AsyncIterator.BufferedIterator(true);
      sourceA._push('A1');
      sourceB = new AsyncIterator.BufferedIterator(true);
      sourceB._push('B1');
      iterator = new UnionIterator([sourceA, sourceB]);
    });

    describe('after the iterator has been created', function () {
      it("should read the first iterator's first element", function () {
        expect(iterator.read()).to.equal('A1');
      });

      it("should read the second iterator's first element", function () {
        expect(iterator.read()).to.equal('B1');
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });

    describe('after both sources have new items', function () {
      before(function () {
        sourceA._push('A2');
        sourceB._push('B2');
      });

      it("should read the first iterator's second element", function () {
        expect(iterator.read()).to.equal('A2');
      });

      it("should read the second iterator's second element", function () {
        expect(iterator.read()).to.equal('B2');
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });

    describe('after only the first source has a new item', function () {
      before(function (done) {
        iterator.once('readable', done);
        sourceA._push('A3');
      });

      it("should read the first iterator's third element", function () {
        expect(iterator.read()).to.equal('A3');
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });

    describe('after only the second source has a new item', function () {
      before(function (done) {
        iterator.once('readable', done);
        sourceB._push('B3');
      });

      it("should read the second iterator's third element", function () {
        expect(iterator.read()).to.equal('B3');
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });

    describe('after both sources have two new items', function () {
      before(function (done) {
        iterator.once('readable', done);
        sourceA._push('A4');
        sourceA._push('A5');
        sourceB._push('B4');
        sourceB._push('B5');
      });

      it("should read the first iterator's fourth element", function () {
        expect(iterator.read()).to.equal('A4');
      });

      it("should read the second iterator's fourth element", function () {
        expect(iterator.read()).to.equal('B4');
      });

      it("should read the first iterator's fifth element", function () {
        expect(iterator.read()).to.equal('A5');
      });

      it("should read the second iterator's fifth element", function () {
        expect(iterator.read()).to.equal('B5');
      });

      it('should should not read another element', function () {
        expect(iterator.read()).to.equal(null);
      });
    });

    describe('after the first source has ended', function () {
      before(function () {
        sourceA._end();
      });

      it('should not have ended', function () {
        iterator.ended.should.be.false;
      });
    });

    describe('after the second source has new items', function () {
      before(function () {
        sourceB._push('B6');
        sourceB._push('B7');
      });

      it("should read the second iterator's sixth element", function () {
        expect(iterator.read()).to.equal('B6');
      });

      it("should read the second iterator's seventh element", function () {
        expect(iterator.read()).to.equal('B7');
      });

      it('should not have ended', function () {
        iterator.ended.should.be.false;
      });
    });

    describe('after the second source has ended', function () {
      before(function (done) {
        iterator.on('end', done);
        sourceB._end();
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });
    });
  });
});
