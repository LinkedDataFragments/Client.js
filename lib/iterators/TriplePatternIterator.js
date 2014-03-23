/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator extends bindings by reading matches for a triple pattern. */

var StreamIterator = require('./Iterator').StreamIterator,
    TransformIterator = require('./Iterator').TransformIterator,
    rdf = require('../rdf/RdfUtil');

// Creates a new TriplePatternIterator
function TriplePatternIterator(parent, pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(parent, pattern, options);
  TransformIterator.call(this, parent);

  options = options || {};
  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = options.fragmentsClient;
  this._fragmentsQueue = [];
}
TransformIterator.inherits(TriplePatternIterator);

TriplePatternIterator.prototype._read = function (push) {
  // Find a readable fragment
  var fragment = this._fragmentsQueue[0];
  while (fragment && fragment.ended)
    fragment = this._fragmentsQueue.shift();
  // If no readable fragment was found, load another
  if (!fragment) {
    if (this._source.ended) return this._end();
    var bindingsContext = this._source.read();
    return bindingsContext && this._loadFragment(bindingsContext.bindings);
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

// Flushing happens when the last fragment has been read
TriplePatternIterator.prototype._flush = function (push) { };

// Loads a fragment with triples that match the binding of the iterator's triple pattern
TriplePatternIterator.prototype._loadFragment = function (bindings) {
  // Apply the bindings to the iterator's triple pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Retrieve and queue the fragment that corresponds to the resulting pattern
  var fragment = new StreamIterator(this._client.getFragmentByPattern(boundPattern));
  this._fragmentsQueue.push(fragment);
  fragment.bindings = bindings;
  fragment.on('error',    this.emit.bind(this, 'error'));
  fragment.on('readable', this._pushBound);
  fragment.on('end',      this._pushBound);
};

module.exports = TriplePatternIterator;
