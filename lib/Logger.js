/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A Logger provides debug output */

var lastUriPart = /[^\/#]*$/;

// Creates a new Logger
function Logger(name) {
  this._prefix = name + ':';
}

Logger.prototype = {
  _format: function (item) {
    // don't format falsy items
    if (!item)
      return item;
    // format an array
    if (item instanceof Array)
      return item.map(this._format, this);
    // format a triple
    if (item.subject && item.predicate && item.object)
      return [lastUriPart.exec(item.subject)[0],
              lastUriPart.exec(item.predicate)[0],
              lastUriPart.exec(item.object)[0]].join(' ');
    // return any other item unaltered
    return item;
  },

  // Adds an information message
  info: function () {
    var items = Array.prototype.map.call(arguments, this._format, this);
    items.unshift(this._prefix);
    console.log.apply(console, items);
  },
};

module.exports = Logger;
