/**
 * Created by joachimvh on 11/09/2014.
 */
/* This object clusters all nodes that contain a specific variable v. */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil');

function Cluster(v) {
  this.v = v;
  this.nodes = [];
  this.logger = new Logger("Cluster " + v);
  this.logger.disable();
}

// Determine which node needs to be called to maximally improve the results of all nodes that need new values for v.
Cluster.prototype.vote = function (callback) {
  var suppliers = this.suppliers();
  // hungry streams: bind streams with no bind values (i.e. can't be used to find new values until they receive new bindings)
  suppliers = _.filter(suppliers, function (node) { return !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });

  // prioritize filters
  var filters = _.filter(this.filterNodes(), function (node) { return !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });
  if (filters.length > 0)
    return callback(filters[0]);

  if (suppliers.length === 0)
    return callback(null);

  var vote;
  // not enough data yet, need to use other heuristics
  var emptySuppliers = _.filter(suppliers, function (node) { return node.activeStream.triples.length === 0; });
  if (emptySuppliers.length > 0)
    vote = ClusteringUtil.infiniMin(emptySuppliers, function (node) { return node.activeStream.cost; });

  // no use reading from a low estimate, will probably decrease the estimate even more?
  if (!vote)
    //vote = ClusteringUtil.infiniMin(suppliers, function (node) { return _.size(node.activeStream.triples); });
    //vote = _.max(suppliers, function (node) { return node.activeStream.count/node.activeStream.triples.length; });
    vote = _.min(suppliers, function (node) { return node.triples.length; });

  callback(vote);
};

Cluster.prototype.filterNodes = function () {
  // supplies no vars but is in this cluster ==> filter node
  return _.filter(this.nodes, function (node) { return node.activeSupplyVars().length === 0; });
};

Cluster.prototype.suppliers = function () {
  var v = this.v;
  return _.filter(this.nodes, function (node) { return node.supplies(v); });
};

// All variables the suppliers of this cluster are supplied by.
Cluster.prototype.dependsOn = function () {
  return _.filter(_.flatten(_.uniq(_.map(this.suppliers(), function (node) { return node.activeStream.bindVar; }))));
};

module.exports = Cluster;