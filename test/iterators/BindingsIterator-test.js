/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var BindingsIterator = require('../../lib/iterators/BindingsIterator');

var Iterator = require('../../lib/iterators/Iterator');

describe('BindingsIterator', function () {
  describe('The BindingsIterator module', function () {
    it('should make BindingsIterator objects', function () {
      BindingsIterator().should.be.an.instanceof(BindingsIterator);
    });

    it('should be a BindingsIterator constructor', function () {
      new BindingsIterator().should.be.an.instanceof(BindingsIterator);
    });

    it('should make Iterator objects', function () {
      BindingsIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new BindingsIterator().should.be.an.instanceof(Iterator);
    });
  });

  describe('A BindingsIterator without source', function () {
    var iterator;
    before(function () {
      iterator = new BindingsIterator();
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A BindingsIterator with an empty source iterator', function () {
    var iterator;
    before(function () {
      iterator = new BindingsIterator([Iterator.empty()]);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A BindingsIterator with a null source iterator', function () {
    var iterator;
    before(function () {
      iterator = new BindingsIterator([null]);
    });

    it('should have ended', function () {
      iterator.ended.should.be.true;
    });
  });

  describe('A BindingsIterator without _createBindingsTransformer', function () {
    var iterator;
    before(function () {
      iterator = new BindingsIterator(Iterator.single({ '?a': 'a' }));
    });

    it('should not have ended', function () {
      iterator.ended.should.be.false;
    });

    it('should throw an error on read', function () {
      (function () { iterator.read(); })
        .should.throw('The _createBindingsTransformer method has not been implemented.');
    });
  });

  describe('A BindingsIterator with a two-element bindings source', function () {
    var iterator, endEventEmitted = 0;
    before(function () {
      var source = new Iterator.fromArray([{ '?a': 'a' }, { '?a': 'b' }]);
      var transformerA = new Iterator.fromArray([{ '?a': 'a1'}, { '?a': 'a2' }]);
      var transformerB = new Iterator.fromArray([{ '?a': 'b1'}, { '?a': 'b2' }, { '?a': 'b3' }]);

      iterator = new BindingsIterator(source);
      iterator.on('end', function () { endEventEmitted++; });

      var create = sinon.stub(iterator, '_createBindingsTransformer');
      create.onCall(0).returns(transformerA);
      create.onCall(1).returns(transformerB);
      create.onCall(2).throws('_createBindingsTransformer may only be called twice');
    });

    describe('before reading starts', function () {
      it('should not have called _createBindingsTransformer', function () {
        iterator._createBindingsTransformer.should.not.have.been.called;
      });

      it('should not have ended', function () {
        iterator.ended.should.be.false;
      });

      it('should not have emitted the end event', function () {
        endEventEmitted.should.equal(0);
      });
    });

    describe('when reading the first element', function () {
      it('should read the first element from the first transformer', function () {
        expect(iterator.read()).to.deep.equal({ '?a': 'a1'});
      });

      it('should have called _createBindingsTransformer twice', function () {
        iterator._createBindingsTransformer.should.have.been.calledTwice;
      });

      it('should not have ended', function () {
        iterator.ended.should.be.false;
      });

      it('should not have emitted the end event', function () {
        endEventEmitted.should.equal(0);
      });
    });

    describe('when reading the remaining elements', function () {
      it('should read the second element from the first transformer', function () {
        expect(iterator.read()).to.deep.equal({ '?a': 'a2'});
      });

      it('should read the first element from the second transformer', function () {
        expect(iterator.read()).to.deep.equal({ '?a': 'b1'});
      });

      it('should read the second element from the second transformer', function () {
        expect(iterator.read()).to.deep.equal({ '?a': 'b2'});
      });

      it('should read the third element from the second transformer', function () {
        expect(iterator.read()).to.deep.equal({ '?a': 'b3'});
      });

      it('should have made no more calls to _createBindingsTransformer', function () {
        iterator._createBindingsTransformer.should.have.been.calledTwice;
      });
    });

    describe('after reading all element', function () {
      it('should not read more elements', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have ended', function () {
        iterator.ended.should.be.true;
      });

      it('should have emitted the end event', function () {
        endEventEmitted.should.equal(1);
      });
    });
  });
});
