/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator extends bindings by reading matches for a triple pattern. */

var Duplex = require('stream').Duplex,
    rdf = require('../rdf/RdfUtil'),
    _ = require('lodash');

// Creates a new TriplePatternIterator
function TriplePatternIterator(pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(pattern, options);
  Duplex.call(this, { objectMode: true });

  // Set the options
  options = _.defaults(options || {}, {});
  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = options.fragmentsClient;
}
TriplePatternIterator.prototype = _.create(Duplex.prototype);

// Processes a binding by loading a fragment for the bound triple pattern
TriplePatternIterator.prototype._write = function (bindings, encoding, done) {
  // `null`, pushed by `this.end()`, signals the end of the stream
  if (bindings === null) {
    this._fragment = null;
    this.push(null);
    done();
  }

  // Read triples from the fragment
  this._fragment = this._client.getFragmentByPattern(this._pattern);
  this._fragment.once('readable', this.emit.bind(this, 'readable'));
  this._fragment.once('end', done);
};

// Creates bindings by reading a triple from the fragment
TriplePatternIterator.prototype._read = function (size) {
  // Read the fragment until we find a matching triple
  var fragment = this._fragment, triple;
  do {
    triple = fragment && fragment.read();
    if (!triple)
      return fragment && fragment.once('readable', this.emit.bind(this, 'readable'));
  }
  while (!this._patternFilter(triple));

  // Emit the bindings for the given triple
  var tripleBindings = rdf.findBindings(this._pattern, triple);
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
