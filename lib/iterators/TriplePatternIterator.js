/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TriplePatternIterator stream bindings for a triple pattern. */

var Transform = require('stream').Transform,
    rdf = require('../rdf/RdfUtil'),
    _ = require('lodash');

// Creates a new TriplePatternIterator
function TriplePatternIterator(pattern, options) {
  if (!(this instanceof TriplePatternIterator))
    return new TriplePatternIterator(pattern, options);
  Transform.call(this, { objectMode: true });

  // Set the options
  options = _.defaults(options || {}, {});
  this._pattern = pattern;
  this._patternFilter = rdf.tripleFilter(pattern);
  this._client = options.fragmentsClient;

  // Pause the iterator, so the fragment is not needlessly retrieved
  this.pause();
  var oldResume = this.resume;
  this.resume = function () {
    this._loadNextFragment();
    this.resume = oldResume;
    this.resume();
  };
}

// Inherit from Transform
TriplePatternIterator.prototype = _.create(Transform.prototype);

// Load the next fragment
TriplePatternIterator.prototype._loadNextFragment = function () {
  this._fragment = this._client.getFragmentByPattern(this._pattern);
  this._fragment.pipe(this);
};

// Transform a stream of triples into a stream of bindings for the pattern
TriplePatternIterator.prototype._transform = function (triple, encoding, done) {
  if (this._patternFilter(triple)) {
    var tripleBindings = rdf.findBindings(this._pattern, triple);
    this.push({ bindings: tripleBindings });
  }
  done();
};

module.exports = TriplePatternIterator;
