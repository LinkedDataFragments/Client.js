/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator builds bindings by reading matches for a triple pattern. */

var BindingsIterator = require('./BindingsIterator'),
    rdf = require('../util/RdfUtil');

// Creates a new TriplePatternIterator
function TriplePatternIterator(parent, pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(parent, pattern, options);
  BindingsIterator.call(this, parent, options);

  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = this._options.fragmentsClient;
}
BindingsIterator.inherits(TriplePatternIterator);

// Creates a fragment with triples that match the binding of the iterator's triple pattern
TriplePatternIterator.prototype._createBindingsTransformer = function (bindings, options) {
  // Apply the bindings to the iterator's triple pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Retrieve the fragment that corresponds to the resulting pattern
  return this._client.getFragmentByPattern(boundPattern);
};

// Reads a binding from the given fragment
TriplePatternIterator.prototype._readBindingsTransformer = function (fragment) {
  // Push a triple that leads to consistent bindings
  var triple, tripleBindings = null;
  do {
    // Find the next triple in the fragment that matches this iterator's triple pattern
    do { if (!(triple = fragment.read())) return; }
    while (!this._patternFilter(triple));
    // Add the triple's bindings to the bindings used to retrieve the fragment
    try {
      tripleBindings = rdf.extendBindings(fragment.bindings, this._pattern, triple);
      this._push(tripleBindings);
    }
    catch (bindingError) { /* bindings weren't consistent */ }
  }
  while (tripleBindings === null);
};

// Generates a textual representation of the iterator
TriplePatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + rdf.toQuickString(this._pattern) + ')}' +
         '\n  <= ' + this.getSourceString();
};

module.exports = TriplePatternIterator;
