/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A ReorderingGraphPatternIterator builds bindings by reading matches for a basic graph pattern. */

var Iterator = require('../iterators/Iterator'),
    MultiTransformIterator = require('../iterators/MultiTransformIterator'),
    rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    Logger = require('../util/ExecutionLogger')('ReorderingGraphPatternIterator');

var TriplePatternIterator = require('./TriplePatternIterator');

// Creates a new ReorderingGraphPatternIterator
function ReorderingGraphPatternIterator(parent, pattern, options) {
  // Empty patterns have no effect; return a pass-through iterator
  if (!pattern || !pattern.length)
    return new Iterator.passthrough(parent, options);
  // A one-element pattern can be solved by a triple pattern iterator
  if (pattern.length === 1 && !options.filters)
    return new TriplePatternIterator(parent, pattern[0], options);
  // For length two or more, construct a ReorderingGraphPatternIterator
  if (!(this instanceof ReorderingGraphPatternIterator))
    return new ReorderingGraphPatternIterator(parent, pattern, options);
  MultiTransformIterator.call(this, parent, options);

  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
  this._filters = this._options.filters || {};
  this._filterCount = _.reduce(this._filters, function (sum, n, key) { return sum + n.length; }, 0);
}
MultiTransformIterator.inherits(ReorderingGraphPatternIterator);

// Creates a pipeline with triples matching the binding of the iterator's graph pattern
ReorderingGraphPatternIterator.prototype._createTransformer = function (bindings, options) {
  // Apply the context bindings to the iterator's graph pattern
  var boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Select the smallest connected subpattern with the least number of unique variables in the resulting pattern
  var subPatterns = _.sortBy(rdf.findConnectedPatterns(boundPattern), function (patterns) {
        var distinctVariableCount = _.union.apply(_, patterns.map(rdf.getVariables)).length;
        return -(boundPattern.length * distinctVariableCount + patterns.length);
      }),
      subPattern = subPatterns.pop(), remainingPatterns = subPattern.length, pipeline;

  // If this subpattern has only one triple pattern, use it to create the pipeline
  if (remainingPatterns === 1 && !this._filters)
    return createPipeline(subPattern.pop());

  // Otherwise, we must first find the best triple pattern to start the pipeline
  pipeline = new Iterator.PassthroughIterator(true);
  // Retrieve and inspect the triple patterns' metadata to decide which has least matches
  var bestIndex = 0, minMatches = Infinity, bestFilter = null;
  subPattern.forEach(function (triplePattern, index) {
    var fragment = this._client.getFragmentByPattern(triplePattern);
    fragment.getProperty('metadata', function (metadata) {
      Logger.logBinding(this, bindings, triplePattern, metadata.totalTriples);
      // We don't need more data from the fragment
      fragment.close();
      // If there are no matches, the entire graph pattern has no matches
      if (metadata.totalTriples === 0)
        return pipeline._end();
      // This triple pattern is the best if it has the lowest number of matches
      if (metadata.totalTriples < minMatches)
        bestIndex = index, minMatches = metadata.totalTriples, bestFilter = null;

      var callback = function () {
        // After all patterns were checked, create the pipeline from the best pattern
        if (--remainingPatterns === 0)
          pipeline.setSource(createPipeline(subPattern.splice(bestIndex, 1)[0], bestFilter));
      };

      var vars = rdf.getVariables(triplePattern);
      var filters = _.flatten(vars.map(function (v) { return this._filters[v] || []; }, this));

      if (filters.length > 0) {
        callback = _.after(this._filterCount, callback);
        var self = this;
        filters.forEach(function (filter) {
          filter.totalTriples(function (count) {
            // TODO: hardcoded pagesize
            if (count * 100 < minMatches)
              bestIndex = index, minMatches = count * 100, bestFilter = filter;
            callback();
          });
        });
      } else {
        callback();
      }

    }, this);
    // If the fragment errors, pretend it was empty
    fragment.on('error', function (error) {
      Logger.warning(error.message);
      if (!fragment.getProperty('metadata'))
        fragment.setProperty('metadata', { totalTriples: 0 });
    });
  }, this);
  return pipeline;

  // Creates the pipeline of iterators for the bound graph pattern,
  // starting with a TriplePatternIterator for the triple pattern,
  // then a ReorderingGraphPatternIterator for the remainder of the subpattern,
  // and finally, ReorderingGraphPatternIterators for the remaining subpatterns.
  function createPipeline(triplePattern, filter) {
    // Create the iterator for the triple pattern
    var startIterator = Iterator.single(bindings),
        pipeline = new TriplePatternIterator(filter ? filter.create(startIterator, options) : startIterator, triplePattern, options);
    // If the chosen subpattern has more triples, create a ReorderingGraphPatternIterator for it
    if (subPattern && subPattern.length !== 0)
      pipeline = new ReorderingGraphPatternIterator(pipeline, subPattern, options);
    // Create ReorderingGraphPatternIterators for all interconnected subpatterns
    while (subPattern = subPatterns.pop())
      pipeline = new ReorderingGraphPatternIterator(pipeline, subPattern, options);
    return pipeline;
  }
};

// Generates a textual representation of the iterator
ReorderingGraphPatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + this._pattern.map(rdf.toQuickString).join(' ') + '}]' +
         '\n  <= ' + this.getSourceString();
};

module.exports = ReorderingGraphPatternIterator;
