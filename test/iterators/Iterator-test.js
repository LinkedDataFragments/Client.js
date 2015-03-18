/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var Iterator = require('../../lib/iterators/Iterator');

var EventEmitter = require('events').EventEmitter,
    Readable = require('stream').Readable;

describe('Iterator', function () {
  describe('The Iterator module', function () {
    it('should make Iterator objects', function () {
      Iterator().should.be.an.instanceof(Iterator);
    });

    it('should be a Iterator constructor', function () {
      new Iterator().should.be.an.instanceof(Iterator);
    });

    it('should make EventEmitter objects', function () {
      Iterator().should.be.an.instanceof(EventEmitter);
    });

    it('should be a EventEmitter constructor', function () {
      new Iterator().should.be.an.instanceof(EventEmitter);
    });
  });

  describe('An default Iterator instance', function () {
    var iterator = new Iterator();

    it('should throw an error when trying to read', function () {
      (function () { iterator.read(); })
      .should.throw('The _read method has not been implemented.');
    });
  });

  describe('An Iterator instance pushing 3 items and then null', function () {
    var iterator = new Iterator();
    var items = [1, 2, 3];
    var readCalled = 0;
    iterator._read = function () {
      readCalled++;
      this._push(items.shift() || null);
    };
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should return element 1 on read 1', function () {
      expect(iterator.read()).to.equal(1);
    });

    it('should not have ended after read 1', function () {
      expect(iterator.ended).to.be.false;
    });

    it('should return element 2 on read 2', function () {
      expect(iterator.read()).to.equal(2);
    });

    it('should not have ended after read 2', function () {
      expect(iterator.ended).to.be.false;
    });

    it('should not emit end before read 3', function () {
      endEventEmitted.should.equal(0);
    });

    it('should return element 3 on read 3', function () {
      expect(iterator.read()).to.equal(3);
    });

    it('should emit end after read 3', function () {
      endEventEmitted.should.equal(1);
    });

    it('should have ended after read 3', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should return null on read 4', function () {
      expect(iterator.read()).to.equal(null);
    });

    it('should have ended after read 4', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should return null on read 5', function () {
      expect(iterator.read()).to.equal(null);
    });

    it('should have ended after read 5', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should have called _read only 4 times', function () {
      readCalled.should.equal(4);
    });

    it('should have emited end only once', function () {
      endEventEmitted.should.equal(1);
    });
  });

  describe('An Iterator instance pushing 8 items', function () {
    var iterator = new Iterator();
    var items = [1, 2, 3, 4, 5, 6, 7, 8];
    var readCalled = 0;
    iterator._read = function () {
      readCalled++;
      this._push(items.shift());
    };

    describe('after construction', function () {
      it('should not buffer any elements', function () {
        readCalled.should.equal(0);
      });
    });

    describe('after reading the first element', function () {
      before(function (done) {
        iterator.read();
        setImmediate(done);
      });

      it('should buffer 4 elements in advance', function () {
        readCalled.should.equal(5);
      });
    });
  });

  describe('An Iterator instance pushing 9 items', function () {
    var iterator = new Iterator();
    var items = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    var readCalled = 0;
    iterator._read = function () {
      readCalled++;
      this._push(items.shift());
    };

    describe('after construction', function () {
      it('should not buffer any elements', function () {
        readCalled.should.equal(0);
      });
    });

    describe("after adding a 'readable' listener", function () {
      before(function () {
        iterator.addListener('readable', function () {});
      });

      it('should buffer 4 elements in advance', function () {
        readCalled.should.equal(4);
      });
    });
  });

  describe('An iterator to which a property "abc" is added', function () {
    var iterator = new Iterator();
    before(function () {
      iterator.setProperty('abc', 123);
    });

    it('should return the property value for "abc" without callback', function () {
      iterator.getProperty('abc').should.equal(123);
    });

    it('should return the property value for "abc" through a callback', function (done) {
      iterator.getProperty('abc', function (value) {
        value.should.equal(123);
        done();
      });
    });

    describe('before a property "def" is added', function () {
      it('should return undefined for "def" without callback', function () {
        expect(iterator.getProperty('def')).to.be.undefined;
      });
    });

    describe('after a property "def" is added', function () {
      var valueCallback;
      before(function () {
        iterator.getProperty('def', valueCallback = sinon.spy());
        iterator.setProperty('def', 456);
      });

      it('should have called a pending callback for "def"', function () {
        valueCallback.should.have.been.calledOnce;
        valueCallback.should.have.been.calledWith(456);
      });

      it('should return the property value for "def" without callback', function () {
        iterator.getProperty('def').should.equal(456);
      });

      it('should return the property value for "def" through a callback', function (done) {
        iterator.getProperty('def', function (value) {
          value.should.equal(456);
          done();
        });
      });
    });

    describe('after the iterator has ended', function () {
      var beforeEndCallback;
      before(function (done) {
        iterator.getProperty('xyz', beforeEndCallback = sinon.spy());
        iterator._end();
        iterator.on('end', setImmediate.bind(null, done));
      });

      describe('before a property "xyz" is added', function () {
        it('should return the property value for "abc" without callback', function () {
          iterator.getProperty('abc').should.equal(123);
        });

        it('should return the property value for "abc" through a callback', function (done) {
          iterator.getProperty('abc', function (value) {
            value.should.equal(123);
            done();
          });
        });
      });

      describe('after a property "xyz" is added', function () {
        var afterEndCallback;
        before(function () {
          iterator.getProperty('xyz', afterEndCallback = sinon.spy());
          iterator.setProperty('xyz', 789);
        });

        it('should have called a pending callback for "xyz" attached before end', function () {
          beforeEndCallback.should.have.been.calledOnce;
          beforeEndCallback.should.have.been.calledWith(789);
        });

        it('should have called a pending callback for "xyz" attached after end', function () {
          afterEndCallback.should.have.been.calledOnce;
          afterEndCallback.should.have.been.calledWith(789);
        });

        it('should return the property value for "xyz" without callback', function () {
          iterator.getProperty('xyz').should.equal(789);
        });

        it('should return the property value for "xyz" through a callback', function (done) {
          iterator.getProperty('xyz', function (value) {
            value.should.equal(789);
            done();
          });
        });
      });
    });
  });
});

describe('EmptyIterator', function () {
  var EmptyIterator = Iterator.EmptyIterator;

  describe('The EmptyIterator module', function () {
    it('should make EmptyIterator objects', function () {
      EmptyIterator().should.be.an.instanceof(EmptyIterator);
    });

    it('should be a EmptyIterator constructor', function () {
      new EmptyIterator().should.be.an.instanceof(EmptyIterator);
    });

    it('should make Iterator objects', function () {
      EmptyIterator().should.be.an.instanceof(Iterator);
    });

    it('should be a Iterator constructor', function () {
      new EmptyIterator().should.be.an.instanceof(Iterator);
    });

    it('should be accessible through Iterator.empty()', function () {
      Iterator.empty.should.equal(EmptyIterator);
    });
  });

  describe('An EmptyIterator instance', function () {
    var iterator = new EmptyIterator();
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should have emitted end', function () {
      expect(endEventEmitted).to.equal(1);
    });

    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should not return elements', function () {
      expect(iterator.read()).to.be.null;
    });
  });
});

describe('SingleIterator', function () {
  var SingleIterator = Iterator.SingleIterator;

  describe('The SingleIterator module', function () {
    it('should make SingleIterator objects', function () {
      SingleIterator().should.be.an.instanceof(SingleIterator);
    });

    it('should be a SingleIterator constructor', function () {
      new SingleIterator().should.be.an.instanceof(SingleIterator);
    });

    it('should make Iterator objects', function () {
      SingleIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new SingleIterator().should.be.an.instanceof(Iterator);
    });

    it('should be accessible through Iterator.single()', function () {
      Iterator.single.should.equal(SingleIterator);
    });
  });

  describe('An SingleIterator instance without parameter', function () {
    var iterator = new SingleIterator();
    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('An SingleIterator instance with an item', function () {
    var iterator = new SingleIterator(1);
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should not emit end before read 1', function () {
      endEventEmitted.should.equal(0);
    });

    it('should return element 1 on read 1', function () {
      expect(iterator.read()).to.equal(1);
    });

    it('should emit end after read 1', function () {
      endEventEmitted.should.equal(1);
    });

    it('should have ended after read 1', function () {
      expect(iterator.ended).to.be.true;
    });
  });
});

describe('ArrayIterator', function () {
  var ArrayIterator = Iterator.ArrayIterator;

  describe('The ArrayIterator module', function () {
    it('should make ArrayIterator objects', function () {
      ArrayIterator().should.be.an.instanceof(ArrayIterator);
    });

    it('should be a ArrayIterator constructor', function () {
      new ArrayIterator().should.be.an.instanceof(ArrayIterator);
    });

    it('should make Iterator objects', function () {
      ArrayIterator().should.be.an.instanceof(Iterator);
    });

    it('should be a Iterator constructor', function () {
      new ArrayIterator().should.be.an.instanceof(Iterator);
    });

    it('should be accessible through Iterator.fromArray()', function () {
      Iterator.fromArray.should.equal(ArrayIterator);
    });
  });

  describe('An ArrayIterator instance without arguments', function () {
    var iterator = new ArrayIterator();
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should have emitted end', function () {
      expect(endEventEmitted).to.equal(1);
    });

    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should not return elements', function () {
      expect(iterator.read()).to.be.null;
    });
  });

  describe('An ArrayIterator instance with an empty array as argument', function () {
    var iterator = new ArrayIterator([]);
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should have emitted end', function () {
      expect(endEventEmitted).to.equal(1);
    });

    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should not return elements', function () {
      expect(iterator.read()).to.be.null;
    });
  });

  describe('An ArrayIterator instance with an empty array as argument', function () {
    var iterator = new ArrayIterator([]);
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should have emitted end', function () {
      expect(endEventEmitted).to.equal(1);
    });

    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should not return elements', function () {
      expect(iterator.read()).to.be.null;
    });
  });

  describe('An ArrayIterator instance with a one-element array as argument', function () {
    var iterator = new ArrayIterator([1]);
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should not emit end before read 1', function () {
      endEventEmitted.should.equal(0);
    });

    it('should return element 1 on read 1', function () {
      expect(iterator.read()).to.equal(1);
    });

    it('should emit end after read 1', function () {
      endEventEmitted.should.equal(1);
    });

    it('should have ended after read 1', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('An ArrayIterator instance with a three-element array as argument', function () {
    var iterator = new ArrayIterator([1, 2, 3]);
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should return element 1 on read 1', function () {
      expect(iterator.read()).to.equal(1);
    });

    it('should not have ended after read 1', function () {
      expect(iterator.ended).to.be.false;
    });

    it('should return element 2 on read 2', function () {
      expect(iterator.read()).to.equal(2);
    });

    it('should not have ended after read 2', function () {
      expect(iterator.ended).to.be.false;
    });

    it('should not emit end before read 3', function () {
      endEventEmitted.should.equal(0);
    });

    it('should return element 3 on read 3', function () {
      expect(iterator.read()).to.equal(3);
    });

    it('should emit end after read 3', function () {
      endEventEmitted.should.equal(1);
    });

    it('should have ended after read 3', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should return null on read 4', function () {
      expect(iterator.read()).to.equal(null);
    });

    it('should have ended after read 4', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should return null on read 5', function () {
      expect(iterator.read()).to.equal(null);
    });

    it('should have ended after read 5', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should have emited end only once', function () {
      endEventEmitted.should.equal(1);
    });
  });
});

describe('StreamIterator', function () {
  var StreamIterator = Iterator.StreamIterator;

  describe('The StreamIterator module', function () {
    it('should make StreamIterator objects', function () {
      StreamIterator().should.be.an.instanceof(StreamIterator);
    });

    it('should be a StreamIterator constructor', function () {
      new StreamIterator().should.be.an.instanceof(StreamIterator);
    });

    it('should make Iterator objects', function () {
      StreamIterator().should.be.an.instanceof(Iterator);
    });

    it('should be a Iterator constructor', function () {
      new StreamIterator().should.be.an.instanceof(Iterator);
    });

    it('should be accessible through Iterator.fromStream()', function () {
      Iterator.fromStream.should.equal(StreamIterator);
    });
  });

  describe('A StreamIterator instance with an empty source', function () {
    var iterator = new StreamIterator(Iterator.empty());
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    it('should have emitted end', function () {
      expect(endEventEmitted).to.equal(1);
    });

    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });

    it('should not return elements', function () {
      expect(iterator.read()).to.be.null;
    });
  });

  describe('A StreamIterator instance with an empty stream as argument', function () {
    var stream = new Readable({ objectMode: true });
    stream._read = function () {};
    var iterator = new StreamIterator(stream);
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    describe('before the stream was readable', function () {
      it('should not have emitted end', function () {
        expect(endEventEmitted).to.equal(0);
      });

      it('should not have ended', function () {
        expect(iterator.ended).to.be.false;
      });

      it('should not return elements', function () {
        expect(iterator.read()).to.be.null;
      });
    });

    describe('after the stream was ended', function () {
      stream.push(null);

      it('should have emitted end', function () {
        expect(endEventEmitted).to.equal(1);
      });

      it('should have ended', function () {
        expect(iterator.ended).to.be.true;
      });

      it('should not return elements', function () {
        expect(iterator.read()).to.be.null;
      });
    });
  });

  describe('A StreamIterator instance with a one-element stream as argument', function () {
    var stream = new Readable({ objectMode: true });
    stream._read = function () {};
    var iterator = new StreamIterator(stream);
    var readableEventEmitted = 0;
    iterator.on('readable', function () { readableEventEmitted++; });
    var endEventEmitted = 0;
    iterator.on('end', function () { endEventEmitted++; });

    describe('before the stream was readable', function () {
      it('should not have emitted readable', function () {
        expect(readableEventEmitted).to.equal(0);
      });

      it('should not have emitted end', function () {
        expect(endEventEmitted).to.equal(0);
      });

      it('should not have ended', function () {
        expect(iterator.ended).to.be.false;
      });

      it('should not return elements', function () {
        expect(iterator.read()).to.be.null;
      });
    });

    describe('after the stream became readable', function () {
      before(function (done) {
        iterator.on('readable', done);
        stream.push(1);
      });

      it('should not have emitted end', function () {
        expect(endEventEmitted).to.equal(0);
      });

      it('should not have ended', function () {
        expect(iterator.ended).to.be.false;
      });

      it('should return the element', function () {
        expect(iterator.read()).to.equal(1);
      });
    });

    describe('after the stream has ended', function () {
      before(function (done) {
        stream.push(null);
        stream.on('end', done);
      });

      it('should have emitted end', function () {
        expect(endEventEmitted).to.equal(1);
      });

      it('should have ended', function () {
        expect(iterator.ended).to.be.true;
      });

      it('should not return elements', function () {
        expect(iterator.read()).to.be.null;
      });
    });
  });
});

describe('TransformIterator', function () {
  var TransformIterator = Iterator.TransformIterator;

  describe('The TransformIterator module', function () {
    it('should make TransformIterator objects', function () {
      TransformIterator().should.be.an.instanceof(TransformIterator);
    });

    it('should be a TransformIterator constructor', function () {
      new TransformIterator().should.be.an.instanceof(TransformIterator);
    });

    it('should make Iterator objects', function () {
      TransformIterator().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new TransformIterator().should.be.an.instanceof(Iterator);
    });

    it('should be accessible through Iterator.transform()', function () {
      Iterator.transform.should.equal(TransformIterator);
    });
  });

  describe('A TransformIterator instance with an empty source', function () {
    var iterator = new TransformIterator(Iterator.empty());
    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('A TransformIterator instance with an empty iterator', function () {
    var iterator = new TransformIterator(Iterator.empty());
    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('A TransformIterator instance with an iterator that has already ended', function () {
    var endedIterator = new EventEmitter();
    endedIterator.ended = true;
    var iterator = new TransformIterator(endedIterator);
    it('should have ended', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('A TransformIterator instance without _transform function', function () {
    var iterator = new TransformIterator(Iterator.single(1));
    it('should throw an error on read', function () {
      (function () { iterator.read(); })
      .should.throw('The _transform method has not been implemented.');
    });
  });

  describe('A TransformIterator instance with a single-element iterator', function () {
    var sourceIterator = Iterator.single(1);
    var iterator = new TransformIterator(sourceIterator);
    iterator._transform = function (item, done) {
      this._push('t' + item);
      done();
    };

    it('should return the transformed element 1 on read 1', function () {
      expect(iterator.read()).to.equal('t1');
    });

    it('should have ended after read 1', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('A TransformIterator instance with a three-element iterator', function () {
    var sourceIterator = Iterator.fromArray([1, 2, 3]);
    var iterator = new TransformIterator(sourceIterator);
    iterator._transform = function (item, done) {
      this._push('t' + item);
      done();
    };
    iterator._flush = function () {
      this._push('end');
      this._push(null);
    };

    it('should return the transformed element 1 on read 1', function () {
      expect(iterator.read()).to.equal('t1');
    });

    it('should return the transformed element 2 on read 2', function () {
      expect(iterator.read()).to.equal('t2');
    });

    it('should return the transformed element 3 on read 3', function () {
      expect(iterator.read()).to.equal('t3');
    });

    it('should not have ended after read 3', function () {
      expect(iterator.ended).to.be.false;
    });

    it('should return the flushed element on read 4', function () {
      expect(iterator.read()).to.equal('end');
    });

    it('should have ended after read 4', function () {
      expect(iterator.ended).to.be.true;
    });
  });

  describe('A TransformIterator that pushes asynchonously after the source has ended', function () {
    var sourceIterator = Iterator.single(1);
    var iterator = new TransformIterator(sourceIterator);
    var pendingTransform;
    iterator._transform = sinon.spy(function (item, done) {
      var self = this;
      pendingTransform = function () {
        self._push('t' + item);
        done();
      };
    });

    describe('before the element has been pushed', function () {
      it('should return null on read', function () {
        expect(iterator.read()).to.equal(null);
      });

      it('should have called _transform', function () {
        iterator._transform.should.have.been.calledOnce;
      });

      it('should not have ended', function () {
        expect(iterator.ended).to.be.false;
      });
    });

    describe('after the element has been pushed and done() has been called', function () {
      before(function () { pendingTransform(); });

      it('should return the transformed element 1', function () {
        expect(iterator.read()).to.equal('t1');
      });

      it('should not have called _transform anymore', function () {
        iterator._transform.should.have.been.calledOnce;
      });

      it('should have ended', function () {
        expect(iterator.ended).to.be.true;
      });
    });
  });
});

describe('Iterator cloning', function () {
  describe('A clone of the empty iterator', function () {
    var source = Iterator.empty();
    var clone = source.clone(), cloneEndEmitted = 0;
    clone.on('end', function () { cloneEndEmitted++; });

    it('should have ended', function () {
      expect(clone.ended).to.be.true;
    });

    it('should have emitted end', function () {
      expect(cloneEndEmitted).to.equal(1);
    });
  });

  describe('A clone of an iterator that ends asynchronously', function () {
    var source = new Iterator.PassthroughIterator(true);
    var clone = source.clone(), cloneEndEmitted = 0;
    clone.on('end', function () { cloneEndEmitted++; });

    before(function () {
      source._end();
    });

    it('should have ended', function () {
      expect(clone.ended).to.be.true;
    });

    it('should have emitted end', function () {
      expect(cloneEndEmitted).to.equal(1);
    });
  });

  describe('A CloneIterator to which 3 elements are added', function () {
    var source = new Iterator.PassthroughIterator(true);
    var clone1, clone2, clone1EndEmitted = 0, clone2EndEmitted = 0;

    describe('after adding the first element and creating two clones', function () {
      before(function () {
        source._push(1);

        clone1 = source.clone();
        clone1.on('end', function () { clone1EndEmitted++; });

        clone2 = source.clone();
        clone2.on('end', function () { clone2EndEmitted++; });
      });

      describe('the first clone', function () {
        it('should be an Iterator', function () {
          clone1.should.be.an.instanceof(Iterator);
        });

        it('should return element 1 on read 1', function () {
          expect(clone1.read()).to.equal(1);
        });

        it('should return null on read 2', function () {
          expect(clone1.read()).to.equal(null);
        });
      });

      describe('the second clone', function () {
        it('should be an Iterator', function () {
          clone2.should.be.an.instanceof(Iterator);
        });

        it('should not emit end before read 1', function () {
          clone2EndEmitted.should.equal(0);
        });

        it('should return element 1 on read 1', function () {
          expect(clone2.read()).to.equal(1);
        });

        it('should return null on read 2', function () {
          expect(clone1.read()).to.equal(null);
        });
      });
    });

    describe('after adding the second and third element', function () {
      before(function () {
        source._push(2);
        source._push(3);
        source._end();
      });

      describe('the first clone', function () {
        it('should return element 2 on read 2', function () {
          expect(clone1.read()).to.equal(2);
        });

        it('should return element 3 on read 3', function () {
          expect(clone1.read()).to.equal(3);
        });

        it('should emit end after read 3', function () {
          clone1EndEmitted.should.equal(1);
        });

        it('should have ended after read 3', function () {
          expect(clone1.ended).to.be.true;
        });
      });

      describe('the second clone', function () {
        it('should return element 2 on read 2', function () {
          expect(clone2.read()).to.equal(2);
        });

        it('should return element 3 on read 3', function () {
          expect(clone2.read()).to.equal(3);
        });

        it('should emit end after read 3', function () {
          clone2EndEmitted.should.equal(1);
        });

        it('should have ended after read 3', function () {
          expect(clone2.ended).to.be.true;
        });
      });
    });
  });
});
