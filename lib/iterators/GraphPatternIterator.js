/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A GraphPatternIterator builds bindings by reading matches for a basic graph pattern. */

var Iterator = require('./Iterator'),
    MultiTransformIterator = require('./MultiTransformIterator'),
    rdf = require('../util/RdfUtil'),
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
  MultiTransformIterator.call(this, parent, options);

  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
}
MultiTransformIterator.inherits(GraphPatternIterator);

// Creates a pipeline with triples matching the binding of the iterator's graph pattern
GraphPatternIterator.prototype._createTransformer = function (bindings, options) {
  // Apply the context bindings to the iterator's graph pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Select the smallest connected subpattern in the resulting pattern
  var subPatterns = _.sortBy(rdf.findConnectedPatterns(boundPattern), 'length'),
      subPattern = subPatterns.pop(), pipeline;

  // If this subpattern has only one triple pattern, use it to create the pipeline
  if (subPattern.length === 1)
    return createPipeline(subPattern.pop());

  // Otherwise, we must first find the best triple pattern to start the pipeline
  pipeline = new Iterator.PassthroughIterator(true);
  // Retrieve and inspect the triple patterns' metadata to decide which has least matches
  var bestIndex = 0, minMatches = Infinity, patternsChecked = 0;
  subPattern.forEach(function (triplePattern, index) {
    var fragment = this._client.getFragmentByPattern(triplePattern);
    fragment.getProperty('metadata', function (metadata) {
      // We don't need more data from the fragment
      fragment.close();
      // If there are no matches, the entire graph pattern has no matches
      if (metadata.totalTriples === 0)
        return pipeline._end();
      // This triple pattern is the best if it has the lowest number of matches
      if (metadata.totalTriples < minMatches)
        bestIndex = index, minMatches = metadata.totalTriples;
      // After all patterns were checked, create the pipeline from the best pattern
      if (++patternsChecked === subPattern.length)
        pipeline.setSource(createPipeline(subPattern.splice(bestIndex, 1)[0]));
    });
  }, this);
  return pipeline;

  // Creates the pipeline of iterators for the bound graph pattern,
  // starting with a TriplePatternIterator for the triple pattern,
  // then a GraphPatternIterator for the remainder of the subpattern,
  // and finally, GraphPatternIterators for the remaining subpatterns.
  function createPipeline(triplePattern) {
    // Create the iterator for the triple pattern
    var startIterator = Iterator.single(bindings),
        pipeline = new TriplePatternIterator(startIterator, triplePattern, options);
    // If the chosen subpattern has more triples, create a GraphPatternIterator for it
    if (subPattern && subPattern.length !== 0)
      pipeline = new GraphPatternIterator(pipeline, subPattern, options);
    // Create GraphPatternIterators for all interconnected subpatterns
    while (subPattern = subPatterns.pop())
      pipeline = new GraphPatternIterator(pipeline, subPattern, options);
    return pipeline;
  }
};

// Generates a textual representation of the iterator
GraphPatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + this._pattern.map(rdf.toQuickString).join(' ') + '}]' +
         '\n  <= ' + this.getSourceString();
};

module.exports = GraphPatternIterator;
