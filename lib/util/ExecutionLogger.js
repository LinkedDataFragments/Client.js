/*! @license ©2013 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University
 * */
var Logger = require('./Logger');

function ExecutionLogger(logger) {
  this._logger = logger || Logger('ExecutionLogger');
  this.index = 0;
}

//TODO
ExecutionLogger.prototype.log = function(bindings, triplePattern, count) {
  this._logger.info(this.index, bindings, triplePattern, count, new Date ().toUTCString());
  this.index++;
};

module.exports = ExecutionLogger; 