/*! @license MIT ©2013-2016 Miel Vander Sande, Ghent University - imec */

var Logger = require('./Logger'),
    util = require('util'),
    AsyncIterator = require('asynciterator');

var iteratorId = 0, logId = 0;

AsyncIterator.prototype._uniqueId = function () {
  if (!this.__uniqueid)
    this.__uniqueid = ++iteratorId;
  return this.__uniqueid;
};

function ExecutionLogger(name) {
  if (!(this instanceof ExecutionLogger))
    return new ExecutionLogger(name);
  Logger.call(this, name);
}
util.inherits(ExecutionLogger, Logger);

ExecutionLogger.prototype.logBinding = function (iterator, bindings, triplePattern, count) {
  if (Logger.enabled('debug'))
    this.debug(++logId, iterator._uniqueId(), bindings, triplePattern, count);
};

ExecutionLogger.prototype.logFragment = function (iterator, fragment, bindings) {
  if (Logger.enabled('debug')) {
    var self = this;
    fragment.getProperty('metadata', function (metadata) {
      self.logBinding(iterator, bindings, iterator._pattern, metadata && metadata.totalTriples);
    });
  }
};

module.exports = ExecutionLogger;
