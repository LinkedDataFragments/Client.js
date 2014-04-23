/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A BindingsIterator builds bindings by passing them through transforming iterators. */

var TransformIterator = require('./Iterator').TransformIterator,
    rdf = require('../util/RdfUtil');

// Creates a new BindingsIterator
function BindingsIterator(bindingsSource, options) {
  if (!(this instanceof BindingsIterator))
    return new BindingsIterator(bindingsSource, options);
  TransformIterator.call(this, bindingsSource, options);

  // Keep a copy of the options to create transformers
  this._options = options || {};
  // Queue of transformers, each corresponding to a different binding
  this._transformerQueue = [];
  // Bound event handlers that will be attached to transformers
  var self = this;
  this._emitError = function (error) { self.emit('error', error); };
  this._fillBufferBound = function () { self._fillBuffer(); };
}
TransformIterator.inherits(BindingsIterator);

// Reads a binding from the next readable transformer,
// creating transformers for new bindings as necessary
BindingsIterator.prototype._read = function () {
  // Find the first readable transformer
  var transformer, transformerQueue = this._transformerQueue, bindings;
  while ((transformer = transformerQueue[0]) && transformer.ended)
    transformerQueue.shift();
  // Create new transformers if none were left
  if (!transformer) {
    do {
      // Create a transformer for the next binding
      if (bindings = this._source.read()) {
        transformer = this._createBindingsTransformer(bindings, this._options);
        // If the transformer has items, add it to the queue and listen for events
        if (transformer && !transformer.ended) {
          transformer.bindings = bindings;
          transformerQueue.push(transformer);
          transformer.on('error',    this._emitError);
          transformer.on('readable', this._fillBufferBound);
          transformer.on('end',      this._fillBufferBound);
        }
      }
    } while (bindings && transformerQueue.length < this._maxBufferSize);
    // If no fragments are left, this iterator has ended
    if (!(transformer = transformerQueue[0]))
      return this._source.ended && this._end();
  }
  // Read a binding from the readable transformer
  this._readBindingsTransformer(transformer);
};

// Creates a transform iterator for the given set of bindings.
// A transformer may output several bindings for any given binding.
BindingsIterator.prototype._createBindingsTransformer = function (bindings, options) {
  throw new Error('The _createBindingsTransformer method has not been implemented.');
};

// Reads a binding from the given transform iterator
BindingsIterator.prototype._readBindingsTransformer = function (transformer) {
  var bindings = transformer.read();
  (bindings !== null) && this._push(bindings);
};

// Flushes remaining data after the source has ended
BindingsIterator.prototype._flush = function () {
  // Transformers might still have triples, so try to fill the buffer
  this._fillBuffer();
};

module.exports = BindingsIterator;
