/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator extends bindings by reading matches for a triple pattern. */

var StreamIterator = require('./Iterator').StreamIterator,
    TransformIterator = require('./Iterator').TransformIterator,
    rdf = require('../rdf/RdfUtil');

// Creates a new TriplePatternIterator
function TriplePatternIterator(parent, pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(parent, pattern, options);
  TransformIterator.call(this, parent, options);

  options = options || {};
  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = options.fragmentsClient;
  this._fragmentsBuffer = [];
}
TransformIterator.inherits(TriplePatternIterator);

TriplePatternIterator.prototype._read = function (push) {
  // Find the first readable fragment in the buffer
  var fragment, fragmentsBuffer = this._fragmentsBuffer, bindingsContext;
  while ((fragment = fragmentsBuffer[0]) && fragment.ended)
    fragmentsBuffer.shift();
  // If no readable fragment was left, load new fragments
  if (!fragment) {
    do {
      if (bindingsContext = this._source.read())
        this._bufferFragment(bindingsContext.bindings);
    } while (bindingsContext && fragmentsBuffer.length < this._bufferSize);
    // If no fragments are left, this iterator has ended
    return fragmentsBuffer.length === 0 && this._source.ended && this._end();
  }

  // Push a triple that leads to consistent bindings
  var triple, tripleBindings = null;
  do {
    // Find the next triple in the fragment that matches this iterator's triple pattern
    do { if (!(triple = fragment.read())) return; }
    while (!this._patternFilter(triple));
    // Add the triple's bindings to the bindings used to retrieve the fragment
    try {
      tripleBindings = rdf.extendBindings(fragment.bindings, this._pattern, triple);
      push({ bindings: tripleBindings });
    }
    catch (bindingError) { /* bindings weren't consistent */ }
  }
  while (tripleBindings === null);
};

// Flushes remaining data after the source has ended
TriplePatternIterator.prototype._flush = function (push) {
  // Buffered fragments might still have triples, so try to fill the buffer
  this._fillBufferOrEmitEnd();
};

// Loads a fragment with triples that match the binding of the iterator's triple pattern
TriplePatternIterator.prototype._bufferFragment = function (bindings) {
  // Apply the bindings to the iterator's triple pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Retrieve and queue the fragment that corresponds to the resulting pattern
  var self = this, fragment = StreamIterator(this._client.getFragmentByPattern(boundPattern));
  this._fragmentsBuffer.push(fragment);
  fragment.bindings = bindings;
  fragment.on('error',    function (error) { self.emit('error', error); });
  fragment.on('readable', function () { self._fillBufferOrEmitEnd(); });
  fragment.on('end',      function () { self._fillBufferOrEmitEnd(); });
};

module.exports = TriplePatternIterator;
