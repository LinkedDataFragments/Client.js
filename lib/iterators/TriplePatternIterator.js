/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator extends bindings by reading matches for a triple pattern. */

var Duplex = require('stream').Duplex,
    rdf = require('../rdf/RdfUtil'),
    _ = require('lodash');

// Creates a new TriplePatternIterator
function TriplePatternIterator(pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(pattern, options);
  Duplex.call(this, { objectMode: true, highWaterMark: 4 });

  // Set the options
  options = _.defaults(options || {}, {});
  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = options.fragmentsClient;
}
TriplePatternIterator.prototype = _.create(Duplex.prototype);

// Processes a binding by loading a fragment for the bound triple pattern
TriplePatternIterator.prototype._write = function (bindingsContext, encoding, done) {
  var self = this;
  this._bindingsContext = bindingsContext;

  // `null`, pushed by `this.end()`, signals the end of the stream
  if (bindingsContext === null) {
    this._fragment = null;
    this.push(null);
    return done();
  }

  // Retrieve a fragment to extract triples from
  var fragment = this._fragment = this._client.getFragmentByPattern(this._pattern);
  fragment.on('end', done);
  fragment.on('error', function (error) { self.emit('error'); });
  // If new triples arrive in the fragment, signal that new bindings can be read
  fragment.on('readable', function () {
    // The `readable` event means the last `read` hit the end of the stream
    // and thus didn't push bindings. Therefore `reading` mode will still be on.
    // Switch it off again, so new reads can happen.
    self._readableState.reading = false;
    self.emit('readable');
  });
};

// Creates bindings by reading a triple from the fragment
TriplePatternIterator.prototype._read = function () {
  // Exit if no fragment is loaded
  var fragment = this._fragment;
  if (!fragment) return;

  // Find a triple that leads to consistent bindings
  var triple, upstreamBindings = this._bindingsContext.bindings, tripleBindings = null;
  do {
    // Find the next triple that matches the pattern
    do { if (!(triple = fragment.read())) return; }
    while (!this._patternFilter(triple));

    // Try to bind the triple to the pattern
    try { tripleBindings = rdf.extendBindings(upstreamBindings, this._pattern, triple); }
    catch (bindingError) { /* bindings weren't consistent */ }
  }
  while (tripleBindings === null);

  // Emit the bindings
  this.push({ bindings: tripleBindings });
};

// Ends the stream by queuing `null` for `this._write`
TriplePatternIterator.prototype.end = function (bindings, encoding, done) {
  var self = this;
  // If there are still bindings, write them first
  if (bindings)
    return this.write(bindings, encoding, function () { self.end(null, null, done); });
  // Otherwise, write null to signal the end
  this.write(null, null, function () { Duplex.prototype.end.call(self, done); });
};

module.exports = TriplePatternIterator;
