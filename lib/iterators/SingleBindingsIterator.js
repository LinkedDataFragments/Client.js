/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SingleBindingsIterator generates a stream with a single bindings object. */

var Readable = require('stream').Readable,
    _ = require('lodash');

// Creates a new SingleBindingsIterator
function SingleBindingsIterator(bindings) {
  if (!(this instanceof SingleBindingsIterator))
    return new SingleBindingsIterator(bindings);
  Readable.call(this, { objectMode: true });

  // Push a copy of the bindings object
  bindings = bindings ? _.clone(bindings) : Object.create(null);
  this._read = function () { this.push({ bindings: bindings }); this.push(null); };
}
SingleBindingsIterator.prototype = _.create(Readable.prototype);

module.exports = SingleBindingsIterator;
