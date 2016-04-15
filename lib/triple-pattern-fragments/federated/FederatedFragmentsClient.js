var FragmentsClient = require('../FragmentsClient'),
  Iterator = require('../../iterators/Iterator'),
  rdf = require('../../util/RdfUtil'),
  _ = require('lodash');

function FederatedFragmentsClient(startFragments, options) {
  if (!(this instanceof FederatedFragmentsClient))
    return new FederatedFragmentsClient(startFragments, options);

  // If only one fragment is given, return original client
  if (!_.isArray(startFragments))
    return new FragmentsClient(startFragments, options);
  if (startFragments.length === 1)
    return new FragmentsClient(startFragments[0], options);

  // Create clients for each of the start fragments
  var clients = this._clients = (startFragments || []).map(function (startFragment) {
    var client = new FragmentsClient(startFragment, options);
    client._emptyPatterns = [];
    return client;
  });

  this._options = _.extend({
    // By default, continue query execution if all but one client fail
    allowedFragmentErrors: clients.length - 1,
  }, options);
}

FederatedFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  var fragments = [];
  this._clients.forEach(function (client) {
    // Check whether the pattern is a bound version of a pattern we know to be empty;
    // in that case, a stricter pattern such as the current one cannot have matches.
    var empty = _.some(client._emptyPatterns, rdf.isBoundPatternOf.bind(null, pattern));
    if (!empty) {
      var fragment = client.getFragmentByPattern(pattern);
      fragment.getProperty('metadata', function (metadata) {
        if (metadata.totalTriples === 0)
          client._emptyPatterns.push(pattern);
      });
      fragments.push(fragment);
    }
  });

  return new CompoundFragment(fragments, this._options);
};

// Creates a new compound Triple Pattern Fragment
function CompoundFragment(fragments, options) {
  if (!(this instanceof CompoundFragment))
    return new CompoundFragment(fragments, options);
  Iterator.call(this);

  // If no fragments are given, the fragment is empty
  if (!fragments || !fragments.length)
    return this.empty(), this;

  // Add fragments to queue and initialize metadata
  var pendingFragments = this._pendingFragments = _.indexBy(fragments, getIndex),
      combinedMetadata = this._metadata = { totalTriples: 0 },
      pendingFragmentsCount = fragments.length,
      pendingMetadataCount  = fragments.length,
      allowedErrors = options.allowedFragmentErrors || 0;

  var self = this;
  _.each(pendingFragments, function (fragment, index) {
    fragment.on('readable', emitReadable);

    // Process the metadata of the fragment
    var processMetadata = _.once(function (fragmentMetadata) {
      // Sum the metadata if it exists
      if (fragmentMetadata.totalTriples)
        combinedMetadata.totalTriples += fragmentMetadata.totalTriples;
      // If no metadata is pending anymore, we can emit it
      if (--pendingMetadataCount === 0)
        self.setProperty('metadata', combinedMetadata);
    });
    fragment.getProperty('metadata', processMetadata);

    // Process the end of the fragment
    var fragmentDone = _.once(function () {
      // Remove the fragment from the queue
      delete pendingFragments[index];
      // If no fragments are pending anymore, the iterator ends
      if (--pendingFragmentsCount === 0)
        self._end();
    });
    fragment.on('end', fragmentDone);

    fragment.on('error', function (error) {
      // TODO: Make error handling behavior configurable
      // Only error if all of the sources error
      if (allowedErrors-- === 0)
        return self.emit('error', error);
      // Otherwise, silently assume there are no results
      processMetadata({});
      fragmentDone();
    });
  });

  function emitReadable() { self.emit('readable'); }
}
Iterator.inherits(CompoundFragment);

// Reads an element of the first non-empty child fragment
CompoundFragment.prototype._read = function () {
  if (this._reading) return;
  this._reading = true;

  _.find(this._pendingFragments, function (fragment, item) {
    if (item = fragment.read()) {
      this._push(item);
      return true;
    }
  }, this);

  this._reading = false;
};

// Empties the fragment
CompoundFragment.prototype.empty = function () {
  if (!this.getProperty('metadata'))
    this.setProperty('metadata', { totalTriples: 0 });
  this._end();
};

// Returns a textual representation of the fragment
CompoundFragment.prototype.toString = function () {
  return this.toString() + '{' +
         _.map(this._pendingFragments, function (f) { return f.toString(); }).join(', ') + '}';
};

// Collection iterator that returns the second argument (index)
function getIndex(element, index) { return index; }

module.exports = FederatedFragmentsClient;
