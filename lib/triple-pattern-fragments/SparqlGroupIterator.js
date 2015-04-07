/**
 * Created by joachimvh on 7/04/2015.
 */

var _ = require('lodash'),
    ReorderingGraphPatternIterator = require('./ReorderingGraphPatternIterator'),
    UnionIterator = require('../iterators/UnionIterator'),
    SparqlExpressionEvaluator = require('../util/SparqlExpressionEvaluator'),
    FilterIterator = require('../iterators/FilterIterator'),
    Iterator = require('../iterators/Iterator'),
    RegexIterator = require('./RegexIterator');

// Creates an iterator for a SPARQL group
function SparqlGroupIterator(source, group, options) {

  if (_.isArray(group)) {

    var grouped = _.groupBy(group, function (subGroup) {
      if (subGroup.type === 'filter' && subGroup.expression.operator === 'regex') {
        try {
          var regIt = new RegexIterator(null, subGroup.expression, options);
          // TODO: not exactly the correct place to store these
          if (!options.filters)
            options.filters = {};
          if (!options.filters[regIt.variable])
            options.filters[regIt.variable] = [];
          // TODO: intersection of multiple filters for same variable
          options.filters[regIt.variable].push(regIt);
          return 'substring';
        } catch (e) {
          return 'default';
        }
      } else {
        return 'default';
      }
    });

    group = grouped.default || [];

    return group.reduce(function (source, subGroup) {
      return new SparqlGroupIterator(source, subGroup, options);
    }, source);
  }

  // Reset flags on the options for child iterators
  var childOptions = options.optional ? _.create(options, { optional: false }) : options;

  // TODO: see when filters are out of scope
  switch (group.type) {
  case 'bgp':
    return new ReorderingGraphPatternIterator(source, group.triples, options);
  case 'group':
    return new SparqlGroupIterator(source, group.patterns, childOptions);
  case 'optional':
    childOptions = _.create(options, { optional: true });
    return new SparqlGroupIterator(source, group.patterns, childOptions);
  case 'union':
    return new UnionIterator(group.patterns.map(function (patternToken) {
      return new SparqlGroupIterator(source.clone(), patternToken, childOptions);
    }), options);
  case 'filter':
    // TODO: putting this here is not nice, putting it in FilterIterator is also not nice (since this is something specific for TPF) ...
    // TODO: maybe create a special SparqlExpressionEvaluator? (isn't useful for the new algorithm though ...)
    // A set of bindings matches the filter if it doesn't evaluate to 0 or false
    var evaluate = SparqlExpressionEvaluator(group.expression);
    return new FilterIterator(source, function (bindings) {
      return !/^"false"|^"0"/.test(evaluate(bindings));
    }, options);
  default:
    throw new Error('Unsupported group type: ' + group.type);
  }
}
Iterator.inherits(SparqlGroupIterator);


module.exports = SparqlGroupIterator;