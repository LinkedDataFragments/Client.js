/**
 * Created by joachimvh on 27/08/2014.
 */

var rdf = require('../util/RdfUtil'),
    _ = require('lodash');

function ClusteringUtil() {}

ClusteringUtil.sum = function (vals, pluck) {
  return _.reduce(vals, function (memo, val) {
    return memo + (pluck ? val[pluck] : val);
  }, 0);
};

// problem with lodash min: values are compared with Infinity, which causes incorrect results if one of the values is actually Infinity (values are just equal, not smaller)
ClusteringUtil.infiniMin = function (vals, valFtn) {
  if (_.isEmpty(vals))
    return Infinity; // to stay consistent with _.min
  var best = {obj: vals[0], val: valFtn(vals[0])};
  _.each(_.rest(vals), function (obj) {
    var val = valFtn(obj);
    if (val < best.val) {
      best.obj = obj;
      best.val = val;
    }
  });
  return best.obj;
};

// rdf.toQuickString sometimes gives duplicate results because of accents
ClusteringUtil.tripleID = function (triple) {
  return triple.subject + triple.predicate + triple.object;
};

module.exports = ClusteringUtil;