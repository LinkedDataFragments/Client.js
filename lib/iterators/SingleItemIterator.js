/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SingleItemIterator generates a stream with a single object. */

var Readable = require('stream').Readable,
    _ = require('lodash');

// Creates a new SingleItemIterator
function SingleItemIterator(bindings) {
  if (!(this instanceof SingleItemIterator))
    return new SingleItemIterator(bindings);
  Readable.call(this, { objectMode: true });

  // Push a copy of the bindings object
  bindings = bindings ? _.clone(bindings) : Object.create(null);
  this._read = function () { this.push({ bindings: bindings }); this.push(null); };
}
SingleItemIterator.prototype = _.create(Readable.prototype);

module.exports = SingleItemIterator;
