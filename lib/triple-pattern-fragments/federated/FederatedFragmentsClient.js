var FragmentsClient = require('../FragmentsClient'),
  Iterator = require('../../iterators/Iterator'),
  rdf = require('../../util/RdfUtil'),
  _ = require('lodash');

function FederatedFragmentsClient(startFragments, options) {
  if (!(this instanceof FederatedFragmentsClient))
    return new FederatedFragmentsClient(startFragments, options);

  if (typeof startFragments === 'string')
    startFragments = startFragments.split(options.delimiter || ',');

  // If only one fragement is given, return original client
  if (!_.isArray(startFragments))
    return new FragmentsClient(startFragments, options);

  if (startFragments.length === 1)
    return new FragmentsClient(startFragments[0], options);

  this._clients = (startFragments || []).map(function (startFragment) {
    var client = new FragmentsClient(startFragment, options);
    client._emptyPatterns = [];
    return client;
  });

  this._options = options;
}

FederatedFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  function isBoundPatternOf(child, parent) {
    return (rdf.isVariable(parent.subject) || (parent.subject === child.subject)) && (rdf.isVariable(parent.predicate) || (parent.predicate === child.predicate)) && (rdf.isVariable(parent.object) || (parent.object === child.object));
  }

  var fragments = [];
  this._clients.forEach(function (client) {
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
      fragments.push(fragment);
    }
  });

  return new CompoundFragment(fragments, this._options);
};

// Creates a new compound Triple Pattern Fragment
function CompoundFragment(fragments, options) {
  if (!(this instanceof CompoundFragment))
    return new CompoundFragment();
  Iterator.call(this);

  // Add fragments to queue
  this._fragments = fragments || [];

  // If no fragments are given, fragment is empty
  if (this._fragments.length === 0)
    return this.empty();

  // Initialize all metadata variables
  this._registeredCount = this._fragments.length; // Keep initial number of registered fragments
  this._metadataCount = 0; // Amount of metadata that has arrived
  this._metadata = {
    totalTriples: 0
  };

  // Init iterator lock
  this._reading = false;

  var self = this;
  this._fragments.forEach(function (fragment) {
    fragment.on('error', function (error) {
      // If the error is a timeout, handle it silently
      // since federated queries are more vulnerable to timeouts
      //if (error.code && (error.code === 'ETIMEDOUT' || error.code === 'ESOCKETTIMEDOUT')) {
        // Increment the Metadata, as it is empty
        self._checkMetadata();
        // Remove iterator from registry
        self._deregister(this);
      //} else
        self.emit('warning', error);
    });

    fragment.on('readable', function () {
      self.emit('readable');
    });

    fragment.on('end', function () {
      self._deregister(this);
    });

    fragment.getProperty('metadata', function (fragmentMetadata) {
      // Sum all metadata
      self._metadata.totalTriples += fragmentMetadata.totalTriples;
      self._checkMetadata();
    });
  });
}
Iterator.inherits(CompoundFragment);

CompoundFragment.prototype._checkMetadata = function () {
  this._metadataCount++;
  if (!this.getProperty('metadata') && (this._metadataCount === this._registeredCount))
    this.setProperty('metadata', this._metadata);
};

CompoundFragment.prototype._deregister = function (fragment) {
  // Remove fragment from queue
  var index = this._fragments.indexOf(fragment);
  if (index > -1) this._fragments.splice(index, 1);
  // If all fragments are removed the iterator ends
  if (this._fragments.length === 0)
    this._end();
};

CompoundFragment.prototype._read = function () {
  // If something is already reading, postpone operation
  if (this._reading)
    return;
  // Lock the iterator
  this._reading = true;

  for (var i = 0; i < this._fragments.length; i++) {
    var fragment = this._fragments[i];
    // Try to read
    var item = fragment.read();
    if (item) {
      this._push(item);
      break;
    }
  }
  // Unlock the iterator
  this._reading = false;
};

// Empties the fragment and returns it
CompoundFragment.prototype.empty = function () {
  if (!this.getProperty('metadata'))
    this.setProperty('metadata', { totalTriples: 0 });
  return this._end(), this;
};

CompoundFragment.prototype.toString = function () {
  return this.toString() + '{' + this.fragments.reduce(function (previousValue, fragment) { return previousValue + ',' + fragment.toString(); }) + '}';
};

module.exports = FederatedFragmentsClient;
