/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A MultiTransformIterator transforms items of a source into zero, one, or multiple items.
    To this end, it creates a new transforming iterator for each item it reads from the source. */

var TransformIterator = require('./Iterator').TransformIterator;

// Creates a new MultiTransformIterator
function MultiTransformIterator(source, options) {
  if (!(this instanceof MultiTransformIterator))
    return new MultiTransformIterator(source, options);
  TransformIterator.call(this, source, options);

  // Keep a copy of the options to create transformers, removing the `optional` setting
  this._options = options || {};
  if (this._optional = this._options.optional || false)
    (this._options = Object.create(this._options)).optional = false;
  // Queue of transformers, each corresponding to a different item of the source
  this._transformerQueue = [];
  // Bound event handlers that will be attached to transformers
  var self = this;
  this._emitError = function (error) { self.emit('error', error); };
  this._fillBufferBound = function () { self._fillBuffer(); };
}
TransformIterator.inherits(MultiTransformIterator);

// Keep track of the transformer's status
var WAITING = 0, TRANSFORMING = 1, ENDING = 2;

// Reads an item from the next readable transformer,
// creating transformers for new items as necessary
MultiTransformIterator.prototype._read = function () {
  var item, transformer, transformerQueue = this._transformerQueue, optional = this._optional;

  // Disallow recursive transformations
  if (this._transformStatus !== WAITING) return;
  this._transformStatus = TRANSFORMING;

  // Find the first readable transformer by dequeuing ended transformers
  while ((transformer = transformerQueue[0]) && transformer.ended) {
    transformerQueue.shift();
    // If transforming is optional and the transformer was empty, read the original item
    if (optional) {
      if (!this._itemTransformed) {
        this._push(transformer.item);
        return this._transformStatus = WAITING;
      }
      this._itemTransformed = false;
    }
  }

  // Create new transformers if no readable transformer was left
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
        // If transforming is optional and the transformer is empty, read the original item
        else if (optional) {
          this._push(item);
          return this._transformStatus = WAITING;
        }
      }
    } while (item && transformerQueue.length < this._maxBufferSize);
    // If no fragments are left, this iterator has ended
    if (!(transformer = transformerQueue[0])) {
      if (this._source.ended)
        this._end();
      return this._transformStatus = WAITING;
    }
  }

  // Read an item from the first readable transformer
  item = this._readTransformer(transformer, transformer.item);
  if (item !== null) {
    optional && (this._itemTransformed = true);
    this._push(item);
  }
  this._transformStatus = WAITING;
};

// Creates a transforming iterator for the given item.
// A transformer may output several items per item.
MultiTransformIterator.prototype._createTransformer = function (item, options) {
  throw new Error('The _createTransformer method has not been implemented.');
};

// Reads an item from the given transforming iterator
MultiTransformIterator.prototype._readTransformer = function (transformer, transformerItem) {
  return transformer.read();
};

// Flushes remaining data after the source has ended
MultiTransformIterator.prototype._flush = function () {
  // Transformers might still have items, so try to fill the buffer
  this._fillBuffer();
};

module.exports = MultiTransformIterator;
