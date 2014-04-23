/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator builds bindings by reading matches for a triple pattern. */

var MultiTransformIterator = require('./MultiTransformIterator'),
    rdf = require('../util/RdfUtil');

// Creates a new TriplePatternIterator
function TriplePatternIterator(parent, pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(parent, pattern, options);
  MultiTransformIterator.call(this, parent, options);

  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = this._options.fragmentsClient;
}
MultiTransformIterator.inherits(TriplePatternIterator);

// Creates a fragment with triples that match the binding of the iterator's triple pattern
TriplePatternIterator.prototype._createTransformer = function (bindings, options) {
  // Apply the bindings to the iterator's triple pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Retrieve the fragment that corresponds to the resulting pattern
  return this._client.getFragmentByPattern(boundPattern);
};

// Reads a binding from the given fragment
TriplePatternIterator.prototype._readTransformer = function (fragment) {
  // Read until we find a triple that leads to consistent bindings
  while (true) {
    // Find the next triple in the fragment that matches this iterator's triple pattern
    var triple;
    do { if (!(triple = fragment.read())) return null; }
    while (!this._patternFilter(triple));
    // Add the triple's bindings to the bindings used to retrieve the fragment
    try { return rdf.extendBindings(fragment.item, this._pattern, triple); }
    catch (bindingError) { /* bindings weren't consistent */ }
  }
};

// Generates a textual representation of the iterator
TriplePatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + rdf.toQuickString(this._pattern) + ')}' +
         '\n  <= ' + this.getSourceString();
};

module.exports = TriplePatternIterator;
