/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A UnionIterator returns the results from a set of source iterators. */

var Iterator = require('./Iterator');

// Creates a new UnionIterator from the given sources
function UnionIterator(sources, options) {
  if (!(this instanceof UnionIterator))
    return new UnionIterator(sources, options);
  Iterator.call(this, options);

  // Construct a list of readable sources
  this._sources = [];
  this._sourceIndex = 0;
  if (sources && sources.length) {
    // Create event listeners
    var self = this;
    function fillBuffer()     { self._fillBuffer(); }
    function emitError(error) { self._error(error); }

    // Add all readable sources and listen to their events
    for (var i = 0, l = sources.length; i < l; i++) {
      var source = sources[i];
      if (source && !source.ended) {
        this._sources.push(source);
        source.on('readable', fillBuffer);
        source.on('end',      fillBuffer);
        source.on('error',    emitError);
      }
    }
  }
  // If there are no readable sources, end the iterator
  (this._sources.length === 0) && this._end();
  this._reading = false;
}
Iterator.inherits(UnionIterator);

// Reads items from the sources in a round-robin way
UnionIterator.prototype._read = function () {
  if (this._reading) return;
  this._reading = true;

  var item = null, sources = this._sources, attempts = sources.length;
  // While no item has been found, attempt to read all sources once
  while (!item && attempts--) {
    var source = sources[this._sourceIndex];
    item = source.read();
    // Remove the current source if it has ended
    source.ended ? sources.splice(this._sourceIndex, 1) : this._sourceIndex++;
    (this._sourceIndex < sources.length) || (this._sourceIndex = 0);
  }
  // Push the item if one has been found
  (item !== null) && this._push(item);
  // End the iterator if no readable sources are left
  (this._sources.length === 0) && this._end();
  this._reading = false;
};

module.exports = UnionIterator;
