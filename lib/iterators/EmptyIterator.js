/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** An EmptyIterator generates an empty stream. */

var Readable = require('stream').Readable,
    _ = require('lodash');

// Creates a new EmptyIterator
function EmptyIterator() {
  if (!(this instanceof EmptyIterator))
    return new EmptyIterator();
  Readable.call(this, { objectMode: true });

  // Immediately end the stream
  this._read = function () { this.push(null); };
}
EmptyIterator.prototype = _.create(Readable.prototype);

module.exports = EmptyIterator;
