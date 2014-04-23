/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A MultiTransformIterator transforms items of a source into zero, one, or multiple items.
    To this end, it creates a new transforming iterator for each item it reads from the source. */

var TransformIterator = require('./Iterator').TransformIterator;

// Creates a new MultiTransformIterator
function MultiTransformIterator(source, options) {
  if (!(this instanceof MultiTransformIterator))
    return new MultiTransformIterator(source, options);
  TransformIterator.call(this, source, options);

  // Keep a copy of the options to create transformers
  this._options = options || {};
  // Queue of transformers, each corresponding to a different item of the source
  this._transformerQueue = [];
  // Bound event handlers that will be attached to transformers
  var self = this;
  this._emitError = function (error) { self.emit('error', error); };
  this._fillBufferBound = function () { self._fillBuffer(); };
}
TransformIterator.inherits(MultiTransformIterator);

// Reads an item from the next readable transformer,
// creating transformers for new items as necessary
MultiTransformIterator.prototype._read = function () {
  // Find the first readable transformer
  var transformer, transformerQueue = this._transformerQueue, item;
  while ((transformer = transformerQueue[0]) && transformer.ended)
    transformerQueue.shift();
  // Create new transformers if none were left
  if (!transformer) {
    do {
      // Create a transformer for the next item
      if (item = this._source.read()) {
        transformer = this._createTransformer(item, this._options);
        // If the transformer has items, add it to the queue and listen for events
        if (transformer && !transformer.ended) {
          transformer.item = item;
          transformerQueue.push(transformer);
          transformer.on('error',    this._emitError);
          transformer.on('readable', this._fillBufferBound);
          transformer.on('end',      this._fillBufferBound);
        }
      }
    } while (item && transformerQueue.length < this._maxBufferSize);
    // If no fragments are left, this iterator has ended
    if (!(transformer = transformerQueue[0]))
      return this._source.ended && this._end();
  }
  // Read an item from the readable transformer
  this._readTransformer(transformer);
};

// Creates a transforming iterator for the given item.
// A transformer may output several items per item.
MultiTransformIterator.prototype._createTransformer = function (item, options) {
  throw new Error('The _createTransformer method has not been implemented.');
};

// Reads an item from the given transforming iterator
MultiTransformIterator.prototype._readTransformer = function (transformer) {
  var item = transformer.read();
  (item !== null) && this._push(item);
};

// Flushes remaining data after the source has ended
MultiTransformIterator.prototype._flush = function () {
  // Transformers might still have triples, so try to fill the buffer
  this._fillBuffer();
};

module.exports = MultiTransformIterator;
