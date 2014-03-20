/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A CachedStream caches the contents of a stream for later reuse. */

var PassThrough  = require('stream').PassThrough,
    _ = require('lodash');

// Creates a new CachedStream
function CachedStream(options) {
  if (!(this instanceof CachedStream))
    return new CachedStream();
  PassThrough.call(this, options || { objectMode: true, highWaterMark: 8 });
  this._cache = [];
}
CachedStream.prototype = _.create(PassThrough.prototype);

// Sends data down the pipeline and caches it
CachedStream.prototype._transform = function (chunk, encoding, done) {
  this.push(chunk);
  this._cache.push(chunk);
  done();
};

// Signals the stream has ended
CachedStream.prototype._flush = function (done) {
  this._ended = true;
  done();
};

// Recreates the current stream, starting from the beginning
CachedStream.prototype.clone = function () {
  var stream = this, cache = this._cache, cachePosition = 0,
      clone = new PassThrough(this, this._readableState);
  // Reads data from the clone stream
  clone.read = function () {
    // Read data from the cache when there's still left
    if (cachePosition < cache.length)
      return cache[cachePosition++];
    // Signal if the stream has ended
    if (stream._ended)
      setImmediate(clone.emit.bind(clone, 'end'));
    return null;
  };
  // Signal if new data arrives
  !stream._ended && stream.on('readable', function () {
    setImmediate(function () { clone.emit('readable'); });
  });
  // Recursively add the `clone` method
  clone.clone = function () { return stream.clone(); };
  return clone;
};

module.exports = CachedStream;
