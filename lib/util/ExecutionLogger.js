/*! @license ©2013 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University
 * */
var Logger = require('./Logger'), util = require('util');

function ExecutionLogger(name) {
		Logger.call(this,name);
	  
}
util.inherits(ExecutionLogger,Logger);

//TODO
ExecutionLogger.prototype.logBinding = function (bindings, pattern,triplePattern, count) {
  this.debug(ExecutionLogger.index, pattern, bindings, triplePattern, count);
  ExecutionLogger.index++;
};

ExecutionLogger.index = 1;

module.exports = ExecutionLogger;