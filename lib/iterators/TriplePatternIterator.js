/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator extends bindings by reading matches for a triple pattern. */

var Duplex = require('stream').Duplex,
    rdf = require('../rdf/RdfUtil'),
    _ = require('lodash');

// Creates a new TriplePatternIterator
function TriplePatternIterator(pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(pattern, options);
  Duplex.call(this, { objectMode: true, highWaterMark: 8 });

  options = _.defaults(options || {}, {});
  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = options.fragmentsClient;
  this._fragmentsQueue = [];
}
TriplePatternIterator.prototype = _.create(Duplex.prototype);

// Processes a binding by loading a fragment for the bound triple pattern
TriplePatternIterator.prototype._write = function (bindingsContext, encoding, done) {
  // `null`, pushed by `this.end()`, signals the end of the stream
  if (bindingsContext === null) {
    this._ended = true;
    this._readNextFragment();
    return done();
  }

  // Apply the context bindings to the iterator's triple pattern
  var boundPattern = rdf.applyBindings(bindingsContext.bindings, this._pattern);
  // Retrieve and queue the fragment that corresponds to the resulting pattern
  var fragment = this._client.getFragmentByPattern(boundPattern);
  fragment.bindingsContext = bindingsContext;
  fragment.on('error', this.emit.bind(this));
  this._fragmentsQueue.push(fragment);
  this._readNextFragment();

  // If the buffer has space for more fragments, signal this immediately
  if (this._fragmentsQueue.length < this._writableState.highWaterMark) done();
  // If not, signal it once the fragment has been read
  else fragment.once('end', done);
};

// Starts reading the next fragment in the queue
TriplePatternIterator.prototype._readNextFragment = function () {
  // Don't do anything if we're still reading a previous fragment
  if (this._fragment) return;
  // Don't do anything if no fragments are queued, ending the stream if needed
  if (this._fragmentsQueue.length === 0) return this._ended && this.push(null);

  // Read the fragment
  var self = this, fragment = this._fragment = this._fragmentsQueue.shift();
  fragment.on('readable', function () {
    // A `readable` event means the last `read` did not retrieve fragment data
    // and thus did not push bindings, which left `reading` mode on.
    // To allow new reads to happen, we need to switch `reading` mode off again.
    self._readableState.reading = false;
    self.emit('readable');
  });
  // Read the next fragment when this one is done
  fragment.once('end', function () {
    delete self._fragment;
    self._readNextFragment();
  });
};

// Creates bindings by reading a triple from the fragment
TriplePatternIterator.prototype._read = function () {
  // Exit if no fragment is loaded
  var fragment = this._fragment;
  if (!fragment) return;

  // Find a triple that leads to consistent bindings
  var triple, upstreamBindings = fragment.bindingsContext.bindings, tripleBindings = null;
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
