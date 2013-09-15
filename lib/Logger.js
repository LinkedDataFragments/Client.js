// A Logger provides debug output

var lastUriPart = /[^\/#]*$/;

// Creates a new Logger
function Logger(name) {
  this._prefix = name + ':';
}

Logger.prototype = {
  _format: function (item) {
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
    var items = Array.prototype.map.call(arguments, this._format);
    items.unshift(this._prefix);
    console.log.apply(console, items);
  },
};

module.exports = Logger;
