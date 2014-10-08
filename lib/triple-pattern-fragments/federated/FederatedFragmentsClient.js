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

  return compoundFragment;
};

// Creates a new compound Triple Pattern Fragment
function CompoundFragment() {
  if (!(this instanceof CompoundFragment))
    return new CompoundFragment();
  Iterator.call(this);

  this._fragments = [];
  this._finalized = false;
  this._metadata = null;
  this._endCount = 0;
  this._metadataCount = 0;
}

Iterator.inherits(CompoundFragment);

CompoundFragment.prototype.register = function (fragment) {
  var self = this;
  fragment.on('readable', function () {
    self.emit('readable');
  });

  fragment.on('end', function () {
    self._endCount++;
    //If this was the last fragment to end after close, close self
    if (self._finalized && self._endCount === self._fragments.length)
      self._end();
  });

  fragment.getProperty('metadata', function (fragmentMetadata) {

		//calculate weighted sum
    function wsm(responseTime, totalTriples, pageSize) {
      return responseTime * Math.ceil(totalTriples / pageSize);
    }

    fragment.getProperty('responseTime', function (responseTime) {
      if (self._metadata) {
        // extend current weighted sum
        self._metadata.totalTriples = self._metadata.totalTriples + wsm(responseTime, fragmentMetadata.totalTriples, 1);

      } else {
        self._metadata = { totalTriples: wsm(responseTime, fragmentMetadata.totalTriples, 1)};
      }
    });

    self._metadataCount++;

    if (self._finalized && self._metadataCount === self._fragments.length)
      self.setProperty('metadata', self._metadata);
  });

  fragment.on('error', function (error) { self.emit('error', error); });

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