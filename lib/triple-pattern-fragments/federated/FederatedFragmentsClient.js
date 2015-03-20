var FragmentsClient = require('../FragmentsClient'),
  Iterator = require('../../iterators/Iterator'),
  rdf = require('../../util/RdfUtil'),
  _ = require('lodash');

function FederatedFragmentsClient(startFragments, options) {
  if (!(this instanceof FederatedFragmentsClient))
    return new FederatedFragmentsClient(startFragments, options);

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
  //DEBUG
  this.id = CompoundFragment.stats.length;
  CompoundFragment.stats.push({ id: this.id, metadata: false, ended: false, closed: false, error: [], count: [], endedEnd: 0 });

  // Add fragments to queue
  this._fragments = fragments || [];

  // If no fragments are given, fragment is empty
  if (this._fragments.length === 0)
    return this.empty();

  // Initialize all metadata variables
  this._metadata = {
    totalTriples: 0
  };

  this._reading = false;
//  this._rr = 0;

  this._init();
}
Iterator.inherits(CompoundFragment);
//DEBUG
CompoundFragment.stats = [];
process.on('exit', function(code) {
  CompoundFragment.stats.forEach(function(item) {
    if ((!item.metadata || !item.ended) && !item.closed)
      console.error(item);
  });
});


CompoundFragment.prototype._init = function() {
  var self = this,
      registered = this._fragments.length, // Initial number of registered fragments
      metadataCount = 0; // Amount of metadata that has arrived

  // Remove from queue
  function deregister(fragment) {
    var index = self._fragments.indexOf(fragment);
    if (index > -1) self._fragments.splice(index, 1);
    //If all fragments are removed the iterator can end
    if (self._fragments.length === 0){
      CompoundFragment.stats[self.id].ended = true;//DEBUG
      self._end();
    }
  }

  function checkMetadata() {
    metadataCount++;
    if (!self.getProperty('metadata') && (metadataCount === registered)) {
      CompoundFragment.stats[self.id].metadata = true;
      self.setProperty('metadata', self._metadata);
    }
  }

  this._fragments.forEach(function (fragment) {
    fragment.on('error', function (error) {
      CompoundFragment.stats[self.id].error.push(error);
      // If the error is a timeout, emit timeout event
      if (error.code && (error.code === 'ETIMEDOUT' || error.code === 'ESOCKETTIMEDOUT')) {
        // Increment the Metadata, as it is empty
        checkMetadata();
        // Remove iterator from registry
        CompoundFragment.stats[self.id].endedEnd++; //DEBUG
        deregister(this);
      } else
        self.emit('error', error);
    });

    fragment.on('readable', function () {
      self.emit('readable');
    });

    fragment.on('end', function () {
      CompoundFragment.stats[self.id].endedEnd++; //DEBUG
      deregister(this);
    });

    fragment.getProperty('metadata', function (fragmentMetadata) {
      // Sum all metadata
      self._metadata.totalTriples += fragmentMetadata.totalTriples;
      CompoundFragment.stats[self.id].tripleCount = self._metadata.totalTriples;
      checkMetadata();
    });
    CompoundFragment.stats[self.id].count.push(fragment.toString());
  });
};

CompoundFragment.prototype._read = function () {
  if (this._reading)
    return;

  this._reading = true;

  for (var i = 0; i < this._fragments.length; i++) {
    var fragment = this._fragments[i];
    //console.error('Reading fragment in %d: %d', this.id, this._rr % this._fragments.length)
    //var fragment = this._fragments[this._rr % this._fragments.length];
    //this._rr++;
    // Try to read
    var item = fragment.read();
    if (item) {
      this._push(item);
      break;
    }
  }
  this._reading = false;
};

// Empties the fragment and returns it
CompoundFragment.prototype.empty = function () {
  CompoundFragment.stats[this.id].ended = true;
  CompoundFragment.stats[this.id].metadata = true;
  if (!this.getProperty('metadata'))
    this.setProperty('metadata', { totalTriples: 0 });
  return this._end(), this;
};

CompoundFragment.prototype.toString = function () {
  return this.toString() + '{' + this.fragments.reduce(function (previousValue, fragment) { return previousValue + ',' + fragment.toString(); }) + '}';
};

CompoundFragment.prototype.close = function() {
  CompoundFragment.stats[this.id].closed = true;
  if (!this._ended) {
    this.emit('close');
    this._rejectDataListeners();
  }
};

module.exports = FederatedFragmentsClient;
