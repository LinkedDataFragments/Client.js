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
  return _.find(list, _.partial(_.isEqual, object)) !== undefined;
};

ClusteringUtil.sum = function (vals, pluck) {
  return _.reduce(vals, function (memo, val) {
    return memo + (pluck ? val[pluck] : val);
  }, 0);
};

// problem with lodash min: values are compared with Infinity, which causes incorrect results if one of the values is actually Infinity (values are just equal, not smaller)
ClusteringUtil.infiniMin = function (vals, valFtn) {
  if (_.isEmpty(vals))
    return undefined;
  var best = {obj:vals[0], val:valFtn(vals[0])};
  _.each(_.rest(vals), function (obj) {
    var val = valFtn(obj);
    if (val < best.val) {
      best.obj = obj;
      best.val = val;
    }
  });
  return best.obj;
};

module.exports = ClusteringUtil;