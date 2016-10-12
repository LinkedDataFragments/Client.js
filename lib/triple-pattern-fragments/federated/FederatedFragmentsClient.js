/*! @license MIT ©2015-2016 Miel Vander Sande, Ghent University - imec */

var FragmentsClient = require('../FragmentsClient'),
    BufferedIterator = require('asynciterator').BufferedIterator,
    rdf = require('../../util/RdfUtil'),
    _ = require('lodash');

function FederatedFragmentsClient(startFragments, options) {
  if (!(this instanceof FederatedFragmentsClient))
    return new FederatedFragmentsClient(startFragments, options);

  // If only one fragment is given, create a simple client instead
  if (!_.isArray(startFragments))
    return new FragmentsClient(startFragments, options);
  if (startFragments.length === 1)
    return new FragmentsClient(startFragments[0], options);

  // Create clients for each of the start fragments
  var clients = this._clients = (startFragments || []).map(function (startFragment) {
    var client = new FragmentsClient(startFragment, options);
    client._emptyPatterns = []; // patterns without matches
    return client;
  });

  // Set the default options
  this._options = _.extend({
    errorThreshold: clients.length - 1, // continue if all but one client fail
  }, options);
}

FederatedFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  var fragments = [];
  this._clients.forEach(function (client) {
    // Check whether the pattern is a bound version of a pattern we know to be empty;
    // if so, the current (more specific) pattern will not have matches either.
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

/** Aborts all requests. */
FederatedFragmentsClient.prototype.abortAll = function () {
  this._clients.forEach(function (client) { client.abortAll(); });
};

// Creates a new compound Triple Pattern Fragment
function CompoundFragment(fragments, options) {
  if (!(this instanceof CompoundFragment))
    return new CompoundFragment(fragments, options);
  BufferedIterator.call(this, options);

  // If no fragments are given, the fragment is empty
  if (!fragments || !fragments.length)
    return this.empty(), this;

  // Index fragments for processing and initialize metadata
  var fragmentsPending = fragments.length,
      metadataPending  = fragments.length,
      errorThreshold   = options.errorThreshold || 0,
      combinedMetadata = this._metadata = { totalTriples: 0 };
  fragments = this._fragments = _.indexBy(fragments, getIndex);

  // Combine all fragments into a single fragment
  var compoundFragment = this;
  _.each(fragments, function (fragment, index) {
    fragment.on('readable', setReadable);

    // Process the metadata of the fragment
    var processMetadata = _.once(function (metadata) {
      // Sum the metadata if it exists
      if (metadata.totalTriples)
        combinedMetadata.totalTriples += metadata.totalTriples;
      // If no metadata is pending anymore, we can emit it
      if (--metadataPending === 0)
        compoundFragment.setProperty('metadata', combinedMetadata);
    });
    fragment.getProperty('metadata', processMetadata);

    // Process the end of the fragment
    var fragmentDone = _.once(function () {
      // Remove the fragment from the queue
      delete fragments[index];
      // If no fragments are pending anymore, the iterator ends
      if (--fragmentsPending === 0)
        compoundFragment._end();
    });
    fragment.once('end', fragmentDone);

    // Process a fragment error
    fragment.once('error', function (error) {
      // Only error if the threshold across fragments has been reached
      if (errorThreshold-- === 0)
        return compoundFragment.emit('error', error);
      // Otherwise, silently assume this fragment has no results
      processMetadata({});
      fragmentDone();
    });
  });

  // Make the compound fragment become readable
  function setReadable() { compoundFragment.readable = true; }
}
BufferedIterator.subclass(CompoundFragment);

// Reads elements of the first non-empty child fragments
CompoundFragment.prototype._read = function (count, done) {
  var fragments = this._fragments;
  for (var index in fragments) {
    var fragment = fragments[index], item;
    // Try to read as much items from the fragment as possible
    while (count > 0 && (item = fragment.read()))
      this._push(item), count--;
    // Stop if we have read sufficient elements
    if (!count) break;
  }
  done();
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
         _.map(this._fragments, function (f) { return f.toString(); }).join(', ') + '}';
};

// Collection iterator that returns the second argument (index)
function getIndex(element, index) { return index; }

module.exports = FederatedFragmentsClient;
