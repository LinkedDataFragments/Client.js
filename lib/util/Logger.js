/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A Logger provides debug output */

var rdf = require('./RdfUtil');

// Creates a new Logger
function Logger(name) {
  if (!(this instanceof Logger))
    return new Logger(name);
  this._prefix = name ? name + ':' : '';
}

// Adds an information message
Logger.prototype.info = function () {
  var items = Array.prototype.map.call(arguments, this._format, this);
  items.unshift(this._prefix);
  this._print(items);
};

// Formats the item depending on its type
Logger.prototype._format = function (item) {
  // Don't format falsy items
  if (!item)
    return item;
  // Format an array
  if (item instanceof Array)
    return item.map(this._format, this);
  // Format a triple
  if (item.subject && item.predicate && item.object)
    return rdf.toQuickString(item);
  // Return any other item unaltered
  return item;
};

// Outputs the items to the console
Logger.prototype._print = function (items) {
  console.error.apply(console, items);
};

// Disable all loggers
Logger.disable = function () {
  Logger.prototype.info = function () { };
};

module.exports = Logger;
