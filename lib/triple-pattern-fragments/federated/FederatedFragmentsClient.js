var FragmentsClient = require('../FragmentsClient'),
  Iterator = require('../../iterators/Iterator'),
  rdf = require('../../util/RdfUtil'),
  _ = require('lodash');

function FederatedFragmentsClient(startFragments, options) {
  if (!(this instanceof FederatedFragmentsClient))
    return new FederatedFragmentsClient(startFragments, options);

  if (!_.isArray(startFragments) || (startFragments.length && startFragments.length === 1))
    return new FragmentsClient(startFragments, options);

  this._clients = (startFragments || []).map(function (startFragment) {
    var client = new FragmentsClient(startFragment, options);
    client._emptyPatterns = [];
    return client;
  });
}

FederatedFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  function isBoundPatternOf(child, parent) {
    return (rdf.isVariable(parent.subject) || (parent.subject === child.subject)) && (rdf.isVariable(parent.predicate) || (parent.predicate === child.predicate)) && (rdf.isVariable(parent.object) || (parent.object === child.object));
  }

  var compoundFragment = new CompoundFragment();
  _.each(this._clients, function (client) {
    //Check wether the pattern is bound of an empty pattern
    var empty = _.some(client._emptyPatterns, function (emptyPattern) {
      return isBoundPatternOf(pattern, emptyPattern);
    });

    // if so, bound pattern will also be empty, don't check.
    if (!empty) {
      var fragment = client.getFragmentByPattern(pattern);
      fragment.getProperty('metadata', function (metadata) {
        if (metadata.totalTriples === 0) {
          client._emptyPatterns.push(pattern);
        }
      });
      compoundFragment.register(fragment);
    }
  });
  compoundFragment.finalize();

  return compoundFragment;
};

// Creates a new compound Triple Pattern Fragment
function CompoundFragment() {
  if (!(this instanceof CompoundFragment))
    return new CompoundFragment();
  Iterator.call(this);

  this._fragments = [];
  this._finalized = false;
  this._metadata = {
    totalTriples: 0
  };
  this._endCount = 0;
  this._metadataCount = 0;
}

Iterator.inherits(CompoundFragment);

CompoundFragment.prototype.register = function (fragment) {
  var self = this;
  var totalTriples = null, responseTime = null, pageSize = 1;

  function checkMetadata() {
    self._metadataCount++;

    if (self._finalized && !self.getProperty('metadata') && (self._metadataCount === self._fragments.length)) {
      self.setProperty('metadata', self._metadata);
    }
  }

  function appendSum() {
    //self._metadata.totalTriples = (self._metadata.totalTriples || 0) + (responseTime + 1) * Math.ceil(totalTriples / pageSize);
    self._metadata.totalTriples = (self._metadata.totalTriples || 0) + totalTriples;
    checkMetadata();
  }

  fragment.on('timeout', function (error) {
    var index = self._fragments.indexOf(fragment);
    if (index > -1) self._fragments.splice(index, 1);

    checkMetadata();
  });

  fragment.on('error', function (error) {
    self.emit('error', error);
  });

  fragment.on('readable', function () {
    self.emit('readable');
  });

  fragment.on('end', function () {
    self._endCount++;
    //If this was the last fragment to end after close, close self
    if (self._finalized && self._endCount === self._fragments.length)
      self._end();
  });

  /*fragment.getProperty('responseTime', function (time) {
    responseTime = time;
    if (totalTriples !== null)
      appendSum();
  });*/

  fragment.getProperty('metadata', function (fragmentMetadata) {
    self._metadata.totalTriples += fragmentMetadata.totalTriples;
    console.log(fragmentMetadata);
    checkMetadata();
    //totalTriples = fragmentMetadata.totalTriples;
    //if (responseTime !== null)
      //appendSum();
  });

  this._fragments.push(fragment);
};

CompoundFragment.prototype._read = function () {
  var fragments = this._fragments;

  // If there are no registered fragments, do nothing
  if (!fragments.length)
    return;

  for (var i = 0; i < fragments.length; i++) {
    var fragment = fragments[i];
    // If the fragment ended, skip it.
    if (fragment.ended) continue;

    var item = fragment.read();
    if (item) {
      this._push(item);
      break;
    }
  }
};

// Empties the fragment and returns it
CompoundFragment.prototype.empty = function () {
  if (!this.getProperty('metadata'))
    this.setProperty('metadata', { totalTriples: 0 });
  // Add 1ms response time to Fragments
  if (!this.getProperty('responseTime'))
    this.setProperty('responseTime', 0);
  return this._end(), this;
};

CompoundFragment.prototype.finalize = function () {
  //Method can only be called once
  if (this._finalized)
    return;

  //Prevent any more registrations
  this.register = function () {};

  //If all registered fragments ended, end self.
  (this._endCount === this._fragments.length) && this._end();

  //If all metadata arrived, set property
  (this._metadataCount === this._fragments.length) && this.setProperty('metadata', this._metadata);

  //set finalized to true;
  this._finalized = true;
};

CompoundFragment.prototype.toString = function () {
  return JSON.stringify(this.fragments);
};

module.exports = FederatedFragmentsClient;
