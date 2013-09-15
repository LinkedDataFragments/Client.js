// A Logger provides debug output

// Creates a new Logger
function Logger(name) {
  this._prefix = name + ':';
}

Logger.prototype = {
  // Adds an information message
  info: function () {
    Array.prototype.unshift.call(arguments, this._prefix);
    console.log.apply(console, arguments);
  },
};

module.exports = Logger;
