/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A GraphPatternIterator extends bindings by reading matches for a graph pattern. */

var Duplex = require('stream').Duplex,
    PassThrough = require('stream').PassThrough,
    rdf = require('../rdf/RdfUtil'),
    _ = require('lodash');

var TriplePatternIterator = require('./TriplePatternIterator');

// Creates a new GraphPatternIterator
function GraphPatternIterator(pattern, options) {
  // Empty patterns have no effect; return a pass-through stream
  if (!pattern || !pattern.length)
    return new PassThrough();
  // A one-element pattern can be solved by a triple pattern iterator
  if (pattern.length === 1)
    return new TriplePatternIterator(pattern[0], options);
  // For length two or more, construct a GraphPatternIterator
  if (!(this instanceof GraphPatternIterator))
    return new GraphPatternIterator(pattern, options);
  Duplex.call(this, { objectMode: true, highWaterMark: 8 });

  options = _.defaults(options || {}, {});
  this._pattern = pattern;
  this._options = _.defaults(options || {}, {});
  this._client = options.fragmentsClient;
}
GraphPatternIterator.prototype = _.create(Duplex.prototype);

// Processes a binding by distributing the iterator's graph pattern
// over one TriplePatternIterator and several GraphPatternIterators.
GraphPatternIterator.prototype._write = function (bindingsContext, encoding, done) {
  // `null`, pushed by `this.end()`, signals the end of the stream
  if (bindingsContext === null)
    return this.push(null), done();

  // Apply the context bindings to the iterator's graph pattern
  var boundPattern = rdf.applyBindings(bindingsContext.bindings, this._pattern);
  // Select the smallest connected subpattern
  var subPatterns = _.sortBy(rdf.findConnectedPatterns(boundPattern), 'length'),
      subPattern = subPatterns.pop(), tripleCounts = [], metadataFound = 0;

  // Find the triple pattern in this subpattern that is best to start with
  var self = this, bestIndex = 0, bestCount = Infinity, patternsChecked = 0;
  // If there is only one triple pattern, it is the best
  if (subPattern.length === 1)
    return createIteratorPipeline();
  // If not, the triple pattern with the lowest number of matches is best
  subPattern.forEach(function (triplePattern, index) {
    var fragment = this._client.getFragmentByPattern(triplePattern);
    fragment.getMetadata(function (metadata) {
      // If there are no matches, the entire graph pattern has no matches
      if (metadata.totalTriples === 0)
        return done();
      // This triple pattern is the best if it has the lowest number of matches
      if (metadata.totalTriples < bestCount) {
        bestIndex = index;
        bestCount = metadata.totalTriples;
      }
      // If all patterns have been checked, create the iterator pipeline
      if (++patternsChecked === subPattern.length)
        createIteratorPipeline();
    });
  }, this);

  // Creates a pipeline of iterators,
  // starting with a TriplePatternIterator for the best triple pattern,
  // then GraphPatternIterators for the remainder of that subpattern,
  // and finally, GraphPatternIterators for the remaining interconnected subpatterns.
  function createIteratorPipeline() {
    // Create the iterator for the best triple pattern in the chosen connected subpattern
    var bestTriplePattern = subPattern.splice(bestIndex, 1)[0],
        patternIterator = new TriplePatternIterator(bestTriplePattern, self._options),
        iteratorPipeline = patternIterator;
    // Start the iterator with the binding
    patternIterator.write(bindingsContext);
    patternIterator.end();

    // If the chosen subpattern has more triples, create a GraphPatternIterator for it
    subPattern.length && subPatterns.push(subPattern);

    // Create GraphPatternIterators for all interconnected subpatterns
    while (subPattern = subPatterns.pop()) {
      patternIterator = new GraphPatternIterator(subPattern, self._options);
      iteratorPipeline = iteratorPipeline.pipe(patternIterator);
    }

    // Output bindings from the final iterator
    self._pipeline = iteratorPipeline;
    iteratorPipeline.on('readable', function () {
      // A `readable` event means the last `read` did not retrieve data
      // and thus did not push bindings, which left `reading` mode on.
      // To allow new reads to happen, we need to switch `reading` mode off again.
      self._readableState.reading = false;
      self.emit('readable');
    });
    // Read the next bindings object when this iterator is done
    iteratorPipeline.once('end', function () {
      delete self._pipeline;
      done();
    });
  }
};

// Creates bindings by reading from the pipeline
GraphPatternIterator.prototype._read = function () {
  var bindings = this._pipeline && this._pipeline.read();
  bindings && this.push(bindings);
};

// Ends the stream by queuing `null` for `this._write`
GraphPatternIterator.prototype.end = function (bindingsContext, encoding, done) {
  var self = this;
  // If there are still bindings, write them first
  if (bindingsContext)
    return this.write(bindingsContext, encoding, function () { self.end(null, null, done); });
  // Otherwise, write null to signal the end
  this.write(null, null, function () { Duplex.prototype.end.call(self, done); });
};

module.exports = GraphPatternIterator;
