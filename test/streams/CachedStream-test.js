/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var CachedStream = require('../../lib/streams/CachedStream');
var Stream = require('stream').Stream;

describe('CachedStream', function () {
  describe('The CachedStream module', function () {
    it('should be a function', function () {
      CachedStream.should.be.a('function');
    });

    it('should make CachedStream objects', function () {
      CachedStream().should.be.an.instanceof(CachedStream);
    });

    it('should be a CachedStream constructor', function () {
      new CachedStream().should.be.an.instanceof(CachedStream);
    });

    it('should make Stream objects', function () {
      CachedStream().should.be.an.instanceof(Stream);
    });

    it('should be a Stream constructor', function () {
      new CachedStream().should.be.an.instanceof(Stream);
    });
  });

  describe('A CachedStream', function () {
    describe('when passed three objects', function () {
      var stream = new CachedStream();
      stream.write('a');
      stream.write('b');
      stream.write('c');
      stream.end();

      it('should stream those objects', function (done) {
        stream.should.be.a.streamOf(['a', 'b', 'c'], done);
      });

      describe('a clone of this stream', function () {
        var clone = stream.clone();

        it('should stream those objects', function (done) {
          clone.should.be.a.streamOf(['a', 'b', 'c'], done);
        });

        it('should in turn create clones that stream those objects', function (done) {
          clone.clone().should.be.a.streamOf(['a', 'b', 'c'], done);
        });
      });
    });

    describe('when objects are added after cloning', function () {
      var stream = new CachedStream(), clone, clonedOutput = [];
      stream.write('a');
      stream.write('b');
      clone = stream.clone();

      before(function (done) {
        clone.on('data', function (data) {
          clonedOutput.push(data);
          if (clonedOutput.length === 2) {
            stream.write('c');
            stream.end();
            done();
          }
        });
      });

      describe('the clone', function () {
        it('should stream all objects', function () {
          clonedOutput.should.deep.equal(['a', 'b', 'c']);
        });
      });
    });
  });
});
