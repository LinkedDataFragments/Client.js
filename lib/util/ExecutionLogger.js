/*! @license ©2013 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University
 * */
var Logger = require('./Logger'), util = require('util'), Iterator = require('../iterators/Iterator');

if (typeof Iterator.prototype.uniqueId === 'undefined') {
  var uniqueId = 0;
  Iterator.prototype.uniqueId = function () {
    if (typeof this.__uniqueid === 'undefined')
      this.__uniqueid = ++uniqueId;
    return this.__uniqueid;
  };
}

function ExecutionLogger(name) {
  if (!(this instanceof ExecutionLogger))
    return new ExecutionLogger(name);
  Logger.call(this, name);
}
util.inherits(ExecutionLogger, Logger);

//TODO
ExecutionLogger.prototype.logBinding = function (iterator, bindings, triplePattern, count) {
  if (Logger.enabled('debug'))
    this.debug(ExecutionLogger.index++, iterator.uniqueId(), bindings, triplePattern, count);
};

//TODO
ExecutionLogger.prototype.logFragment = function (iterator, fragment, bindings) {
  if (Logger.enabled('debug')) {
    var self = this;
    fragment.getProperty('metadata', function (metadata) {
      self.logBinding(iterator, bindings, iterator._pattern, metadata && metadata.totalTriples);
    });
  }
};

ExecutionLogger.index = 1;

module.exports = ExecutionLogger;
