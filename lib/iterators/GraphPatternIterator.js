/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A GraphPatternIterator extends bindings by reading matches for a basic graph pattern. */

var Iterator = require('./Iterator'),
    TransformIterator = Iterator.TransformIterator,
    rdf = require('../rdf/RdfUtil'),
    _ = require('lodash');

var TriplePatternIterator = require('./TriplePatternIterator');

// Creates a new GraphPatternIterator
function GraphPatternIterator(parent, pattern, options) {
  // Empty patterns have no effect; return a pass-through iterator
  if (!pattern || !pattern.length)
    return new Iterator.passthrough(parent, options);
  // A one-element pattern can be solved by a triple pattern iterator
  if (pattern.length === 1)
    return new TriplePatternIterator(parent, pattern[0], options);
  // For length two or more, construct a GraphPatternIterator
  if (!(this instanceof GraphPatternIterator))
    return new GraphPatternIterator(parent, pattern, options);
  TransformIterator.call(this, parent, options);

  options = options || {};
  this._options = options;
  this._pattern = pattern;
  this._client = options.fragmentsClient;
  this._pipelines = [];
}
TransformIterator.inherits(GraphPatternIterator);

GraphPatternIterator.prototype._read = function (push) {
  // Find the first readable pipeline
  var pipelines = this._pipelines, pipeline, bindings;
  while ((pipeline = pipelines[0]) && pipeline.ended)
    pipelines.shift();
  // If no readable pipeline was left, load new pipelines
  if (!pipeline) {
    do {
      if (bindings = this._source.read())
        this._addPipeline(bindings);
    } while (bindings && pipelines.length < this._bufferSize);
    // If no pipelines are left, this iterator has ended
    return pipelines.length === 0 && this._source.ended && this._end();
  }

  // Trey to read a triple from the pipeline
  var patternBindings = pipeline.read();
  return patternBindings && push(patternBindings);
};

GraphPatternIterator.prototype._addPipeline = function (bindings) {
  // Apply the context bindings to the iterator's graph pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Select the smallest connected subpattern in the resulting pattern
  var subPatterns = _.sortBy(rdf.findConnectedPatterns(boundPattern), 'length'),
      subPattern = subPatterns.pop(), pipeline;

  // In this pattern, find the best triple pattern to start the pipeline
  var self = this, bestIndex = 0, bestCount = Infinity, patternsChecked = 0;
  // If this subpattern has only one triple pattern, use that for the pipeline
  if (subPattern.length === 1) {
    pipeline = createPipeline();
  }
  // Otherwise, retrieve the triple patterns' metadata to decide which is best
  else {
    pipeline = new Iterator.PassthroughIterator(true);
    subPattern.forEach(function (triplePattern, index) {
      var fragment = this._client.getFragmentByPattern(triplePattern);
      fragment.getProperty('metadata', function (metadata) {
        // If there are no matches, the entire graph pattern has no matches
        if (metadata.totalTriples === 0)
          return pipeline._end();
        // This triple pattern is the best if it has the lowest number of matches
        if (metadata.totalTriples < bestCount)
          bestIndex = index, bestCount = metadata.totalTriples;
        // After all patterns were checked, create the pipeline from the best pattern
        if (++patternsChecked === subPattern.length) {
          pipeline.setSource(createPipeline());
        }
      });
    }, this);
  }
  this._pipelines.push(pipeline);

  // Listen to pipeline events
  pipeline.on('readable', function () { self._fillBufferOrEmitEnd(); });
  pipeline.on('end',      function () { self._fillBufferOrEmitEnd(); });
  pipeline.on('error',    function (error) { self.emit('error', error); });

  // Creates the actual pipeline of iterators for the bound graph pattern,
  // starting with a TriplePatternIterator for the best triple pattern,
  // then a GraphPatternIterator for the remainder of that subpattern,
  // and finally, GraphPatternIterators for the remaining subpatterns.
  function createPipeline() {
    var triplePattern = subPattern.splice(bestIndex, 1)[0];

    // Create the iterator for the triple pattern
    var startIterator = Iterator.single(bindings),
        pipeline = new TriplePatternIterator(startIterator, triplePattern, self._options);

    // If the chosen subpattern has more triples, create a GraphPatternIterator for it
    if (subPattern && subPattern.length !== 0)
      pipeline = new GraphPatternIterator(pipeline, subPattern, self._options);

    // Create GraphPatternIterators for all interconnected subpatterns
    while (subPattern = subPatterns.pop())
      pipeline = new GraphPatternIterator(pipeline, subPattern, self._options);

    return pipeline;
  }
};

// Flushes remaining data after the source has ended
GraphPatternIterator.prototype._flush = function (push) {
  // The pipeline might still have triples, so try to fill the buffer
  this._fillBufferOrEmitEnd();
};

// Generates a textual representation of the iterator
GraphPatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + this._pattern.map(rdf.toQuickString).join(' ') + '}]' +
         '\n  <= ' + this.getSourceString();
};

module.exports = GraphPatternIterator;
