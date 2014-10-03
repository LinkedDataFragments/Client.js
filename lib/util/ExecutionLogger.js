/*! @license ©2013 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University
 * */
var Logger = require('./Logger'), util = require('util'), Iterator = require('../iterators/Iterator');

(function () {
    if (typeof Iterator.prototype.uniqueId == "undefined") {
      var id = 0;
      Iterator.prototype.uniqueId = function () {
        if (typeof this.__uniqueid == "undefined") {
          this.__uniqueid = ++id;
        }
        return this.__uniqueid;
      };
    }
  })();

function ExecutionLogger(name) {
  Logger.call(this, name);
}
util.inherits(ExecutionLogger, Logger);

//TODO
ExecutionLogger.prototype.logBinding = function (objectId, bindings, triplePattern, count) {
  this.debug(ExecutionLogger.index, objectId, bindings, triplePattern, count);
  ExecutionLogger.index++;
};

ExecutionLogger.index = 1;

module.exports = ExecutionLogger;