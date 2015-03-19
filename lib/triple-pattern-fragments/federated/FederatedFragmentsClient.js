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

  var compoundFragment = new CompoundFragment(this._options);
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
      compoundFragment.register(fragment);
    }
  });
  compoundFragment.finalize();

  return compoundFragment;
};

// Creates a new compound Triple Pattern Fragment
function CompoundFragment(options) {
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

  options = options || {};
  // period of time after which the metadata is sent no matter what
  // if (options.timeOut)
  //   this._timeOut = options.timeOut;

  this.id = CompoundFragment.stats.length;
  CompoundFragment.stats.push({ id: this.id, metadata: false, ended: false, error: [], endedStart: 0, metadataStart: 0, endedEnd: 0, metadataEnd: 0, finalized: false });
}

Iterator.inherits(CompoundFragment);

CompoundFragment.stats = [];
process.on('exit', function(code) {
  CompoundFragment.stats.forEach(function(item) {
    if (!item.metadata || !item.ended)
      console.log(item);
  });
});

CompoundFragment.prototype.register = function (fragment) {
  this._fragments.push(fragment);
  var self = this;

  fragment.on('error', function (error) {
    CompoundFragment.stats[self.id].error.push(error);
    // If the error is a timeout, emit timeout event
    if (error.code && (error.code === 'ETIMEDOUT' || error.code === 'ESOCKETTIMEDOUT')) {
      fragment = Iterator.empty();
      // Increment the Metadata, as it is empty
      self._metadataCount++;
      self._checkMetadata();
      // Increment the ended, since empty iterator already ended.
      self._endCount++;
      self._checkEnded();
    } else
      self.emit('error', error);
  });

  fragment.on('readable', function () {
    self.emit('readable');
  });

  fragment.on('end', function () {
    CompoundFragment.stats[self.id].endedEnd++;
    self._endCount++;
    self._checkEnded();
  });

  fragment.getProperty('metadata', function (fragmentMetadata) {
    // Sum all metadata
    self._metadata.totalTriples += fragmentMetadata.totalTriples;
    CompoundFragment.stats[self.id].metadataEnd++;
    self._metadataCount++;
    self._checkMetadata();
  });

  CompoundFragment.stats[this.id].metadataStart++;
  CompoundFragment.stats[this.id].endedStart++;
};

CompoundFragment.prototype._checkMetadata = function () {
  console.log('checkmeta %d: %s %d %d', this.id, this._finalized, this._metadataCount, this._fragments.length);
  if (this._finalized && !this.getProperty('metadata') && (this._metadataCount === this._fragments.length)) {
    CompoundFragment.stats[this.id].metadata = true;
    this.setProperty('metadata', this._metadata);
  }
};

CompoundFragment.prototype._checkEnded = function () {
  console.log('checkend %d: %s %d %d', this.id, this._finalized, this._endCount, this._fragments.length);
  //If this was the last fragment to end after close, close self
  if (this._finalized && this._endCount === this._fragments.length) {
    CompoundFragment.stats[this.id].ended = true;
    this._end();
  }
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
  return this._end(), this;
};

CompoundFragment.prototype.finalize = function () {
  //Method can only be called once
  if (this._finalized === true)
    return;

  //set finalized to true;
  this._finalized = true;

  //Prevent any more registrations
  //this.register = function () {};

  //If all metadata arrived, set property
  this._checkMetadata();

  //If all registered fragments ended, end self.
  this._checkEnded();

  //Add timer if enabled
  // var self = this;
  // if (this._timeOut)
  //   setTimeout(function () {
  //     if (!self.getProperty('metadata'))
  //       self.setProperty('metadata', self._metadata);
  //   }, this._timeOut);
  CompoundFragment.stats[this.id].finalized = this._finalized;
};

CompoundFragment.prototype.toString = function () {
  return JSON.stringify(this.fragments);
};

module.exports = FederatedFragmentsClient;
