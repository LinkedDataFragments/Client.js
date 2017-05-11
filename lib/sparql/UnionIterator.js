/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A UnionIterator returns the results from a set of source iterators. */

var BufferedIterator = require('asynciterator').BufferedIterator;

// Creates a new UnionIterator from the given sources
function UnionIterator(sources, options) {
  if (!(this instanceof UnionIterator))
    return new UnionIterator(sources, options);
  BufferedIterator.call(this, options);

  // Construct a list of readable sources
  this._sources = [];
  this._sourceIndex = 0;
  if (sources && sources.length) {
    // Create event listeners
    var self = this;
    function fillBuffer()     { self._fillBuffer(); }
    function emitError(error) { self.emit('error', error); }

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
}
BufferedIterator.subclass(UnionIterator);

// Reads items from the sources in a round-robin way
UnionIterator.prototype._read = function (count, done) {
  var sources = this._sources, item = null, attempts = sources.length;
  // While no item has been found, attempt to read all sources once
  while (item === null && attempts--) {
    var source = sources[this._sourceIndex];
    item = source.read();
    // Remove the current source if it has ended
    if (source.ended)
      sources.splice(this._sourceIndex, 1);
    // Otherwise, start from the succeeding source next time
    else
      this._sourceIndex++;
    if (this._sourceIndex >= sources.length)
      this._sourceIndex = 0;
  }
  // Push the item if one has been found
  if (item !== null)
    this._push(item);
  // End if no sources are left
  if (!sources.length)
    this.close();
  done();
};

module.exports = UnionIterator;
