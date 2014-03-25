/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator extends bindings by reading matches for a triple pattern. */

var TransformIterator = require('./Iterator').TransformIterator,
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
  this._fragments = [];
}
TransformIterator.inherits(TriplePatternIterator);

TriplePatternIterator.prototype._read = function (push) {
  // Find the first readable fragment
  var fragments = this._fragments, fragment, bindings;
  while ((fragment = fragments[0]) && fragment.ended)
    fragments.shift();
  // If no readable fragment was left, load new fragments
  if (!fragment) {
    do {
      if (bindings = this._source.read())
        this._addFragment(bindings);
    } while (bindings && fragments.length < this._bufferSize);
    // If no fragments are left, this iterator has ended
    return fragments.length === 0 && this._source.ended && this._end();
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
      push(tripleBindings);
    }
    catch (bindingError) { /* bindings weren't consistent */ }
  }
  while (tripleBindings === null);
};

// Loads a fragment with triples that match the binding of the iterator's triple pattern
TriplePatternIterator.prototype._addFragment = function (bindings) {
  // Apply the bindings to the iterator's triple pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Retrieve and queue the fragment that corresponds to the resulting pattern
  var fragment = this._client.getFragmentByPattern(boundPattern), self = this;
  this._fragments.push(fragment);
  fragment.bindings = bindings;
  fragment.on('error',    function (error) { self.emit('error', error); });
  fragment.on('readable', function () { self._fillBufferOrEmitEnd(); });
  fragment.on('end',      function () { self._fillBufferOrEmitEnd(); });
};

// Flushes remaining data after the source has ended
TriplePatternIterator.prototype._flush = function (push) {
  // Buffered fragments might still have triples, so try to fill the buffer
  this._fillBufferOrEmitEnd();
};

// Generates a textual representation of the iterator
TriplePatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + rdf.toQuickString(this._pattern) + ')}' +
         '\n  <= ' + this.getSourceString();
};

module.exports = TriplePatternIterator;
