var FragmentsClient = require('../FragmentsClient'),
    Iterator = require('../../iterators/Iterator'),
    rdf = require('../../util/RdfUtil'),
    _ = require('lodash');

function FederatedFragmentsClient(startFragments, options) {
  if (!(this instanceof FederatedFragmentsClient))
    return new FederatedFragmentsClient(startFragments, options);

  this._clients = (startFragments || []).map(function (startFragment) {
    return new FragmentsClient(startFragment, options);
  });
}

FederatedFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  function isBoundPatternOf(child, parent) {
    return (rdf.isVariable(parent.subject) || (parent.subject === child.subject)) && (rdf.isVariable(parent.predicate) || (parent.predicate === child.predicate)) && (rdf.isVariable(parent.object) || (parent.object === child.object));
  }

  var compoundFragment = new CompoundFragment();

  _.each(this._clients, function (client) {
    var fragment = client.getFragmentByPattern(pattern);
    compoundFragment.register(fragment);
  });
  compoundFragment.finalize();

  return compoundFragment.clone();
};

// Creates a new compound Triple Pattern Fragment
function CompoundFragment() {
  if (!(this instanceof CompoundFragment))
    return new CompoundFragment();
  Iterator.call(this);

  this._fragments = [];
  this._finalized = false;
}

Iterator.inherits(CompoundFragment);

CompoundFragment.prototype.register = function (fragment) {
  var self = this;
  fragment.on('readable', function () {
    self.emit('readable');
  });

  fragment.on('end', function () {
    self._fragments.splice(self._fragments.indexOf(this), 1);
    //If this was the last fragment to end after close, close self
    if (self._finalized && !self._fragments.length)
      self._end();
  });

  fragment.getProperty('metadata', function (fragmentMetadata) {
    var metadata = self.getProperty('metadata');

    if (metadata)
      metadata.totalTriples = metadata.totalTriples + fragmentMetadata.totalTriples;
    // extend current weighted sum
    else
      metadata = fragmentMetadata;

    self.setProperty('metadata', metadata);
  });
  this._fragments.push(fragment);
};

CompoundFragment.prototype._read = function () {
  var fragments = this._fragments;
  // If there are no registered fragments, do nothing
  if (!fragments.length)
    return;

  for (var i = fragments.length - 1; i >= 0; i--) {
    var fragment = fragments[i],
        item = fragment.read();
    if (item) {
      this._push(item);
      break;
    }
    //If it ended, delete it from array
    if (fragment.ended)
      fragments.splice(i, 1);
  }
};

// Empties the compound fragment and returns it
CompoundFragment.prototype.empty = function () {
  return this.single(null);
};

// Adds one single triple to the compound fragment and returns it
CompoundFragment.prototype.single = function (triple) {
  if (!this.getProperty('metadata'))
    this.setProperty('metadata', {
      totalTriples : triple ? 1 : 0
    });
  triple && this._push(triple);
  this._end();
  return this;
};

CompoundFragment.prototype.finalize = function () {
  //Method can only be called once
  if (this._finalized)
    return;

  //Prevent any more registrations
  this.register = function () {
  };

  //If all registered fragments ended, end self.
  !this._fragments.length && this._end();

  //set finalized to true;
  this._finalized = true;

};

CompoundFragment.prototype.toString = function () {
  return JSON.stringify(this.fragments);
};

module.exports = FederatedFragmentsClient;