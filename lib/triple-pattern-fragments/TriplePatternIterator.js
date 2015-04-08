/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A TriplePatternIterator builds bindings by reading matches for a triple pattern. */

var MultiTransformIterator = require('../iterators/MultiTransformIterator'),
    rdf = require('../util/RdfUtil'),
    Logger = require('../util/ExecutionLogger')('TriplePatternIterator'),
    Iterator = require('../iterators/Iterator'),
    BloomFilter = require('bloem').Bloem;

// Creates a new TriplePatternIterator
function TriplePatternIterator(parent, pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(parent, pattern, options);
  MultiTransformIterator.call(this, parent, options);

  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
}
MultiTransformIterator.inherits(TriplePatternIterator);

// Creates a fragment with triples that match the binding of the iterator's triple pattern
TriplePatternIterator.prototype._createTransformer = function (bindings, options) {
  // Apply the bindings to the iterator's triple pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);

  var fragment;
  // If the pattern is a triple, then AMQ can replace an extra request
  if (!rdf.hasVariables(boundPattern)) {
    // Create placeholder for fragment
    fragment = Iterator.passthrough();

    // Retrieve parent fragment (probably in cache) to retrieve filter
    var parentFragment = this._client.getFragmentByPattern(this._pattern),
        self = this;

    parentFragment.getProperty('metadata', function (metadata) {
      if (metadata.filter) {
        var filter = metadata.filter;

        // Get bits & hashes count or estimate them
        var m = filter.bits || Math.ceil((-metadata.totalTriples * Math.log(0.01)) / (Math.LN2 * Math.LN2)),
            k = filter.hashes || Math.round((m / metadata.totalTriples) * Math.LN2),
            bloom = new BloomFilter(m, k, filter.filter); //Initialize bloom filter

        // If binding is member, then request fragment (could be false positive)
        if (bloom.has(Buffer(boundPattern[filter.variable])))
          fragment.setSource(self._client.getFragmentByPattern(boundPattern));
        else
          fragment.setSource(Iterator.empty()); // Binding is not present in set, return empty iterator

      } else
        fragment.setSource(self._client.getFragmentByPattern(boundPattern));
    });
  } else // Retrieve the fragment that corresponds to the resulting pattern
    fragment = this._client.getFragmentByPattern(boundPattern);

  Logger.logFragment(this, fragment, bindings);
  fragment.on('error', function (error) { Logger.warning(error.message); });
  return fragment;
};

// Reads a binding from the given fragment
TriplePatternIterator.prototype._readTransformer = function (fragment, fragmentBindings) {
  // Read until we find a triple that leads to consistent bindings
  var triple;
  while (triple = fragment.read()) {
    // Extend the bindings such that they bind the iterator's pattern to the triple
    try { return rdf.extendBindings(fragmentBindings, this._pattern, triple); }
    catch (bindingError) { /* non-data triple, didn't match the bindings */ }
  }
  // No consistent bindings were available (yet)
  return null;
};

// Generates a textual representation of the iterator
TriplePatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + rdf.toQuickString(this._pattern) + ')}' +
         '\n  <= ' + this.getSourceString();
};

module.exports = TriplePatternIterator;
