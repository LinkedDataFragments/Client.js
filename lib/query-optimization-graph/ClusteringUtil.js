/**
 * Created by joachimvh on 27/08/2014.
 */

var rdf = require('../util/RdfUtil'),
    _ = require('lodash');

function ClusteringUtil(){}

ClusteringUtil.getVariables = function (triple) {
  return _.filter([triple.subject, triple.predicate, triple.object], function(v){ return rdf.isVariable(v); });
};

ClusteringUtil.containsObject = function (list, object) {
  return _.some(list, function (val) {
    return _.where([val], object).length > 0;
  });
};

ClusteringUtil.sum = function (vals, pluck) {
  return _.reduce(vals, function (memo, val) {
    return memo + (pluck ? val[pluck] : val);
  }, 0);
};

module.exports = ClusteringUtil;