/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* Extended by Miel Vander Sande: loosely based on log.js
 * https://github.com/visionmedia/log.js/blob/master/lib/log.js */

var rdf = require('./RdfUtil'),
    csv = require('csv-stringify'),
    _ = require('lodash');

var LOG_LEVELS = ['EMERGENCY', 'ALERT', 'CRITICAL', 'ERROR', 'WARNING', 'NOTICE', 'INFO', 'DEBUG'];

/**
 * Creates a new `Logger`.
 * @constructor
 * @classdesc A `Logger` provides debug output.
 * @param name The name of the logger
 */
function Logger(name) {
  if (!(this instanceof Logger))
    return new Logger(name);
  this._name = name || '';
}

/**
 * Logs the message with the given level
 * @param {String} level The log level
 * @param {...Object} items The items to log
 */
Logger.prototype.log = function (level, items) {
  items = _.map(items, this._format, this);
  items.unshift('[' + new Date() + ']', level, this._name);
  this._print(items);
};

// Add helper log functions for each level
LOG_LEVELS.forEach(function (level) {
  Logger.prototype[level.toLowerCase()] = function () { this.log(level, arguments); };
});

/**
 * Formats the item for logging, depending on its type.
 * @protected
 * @param {Object} item The item to format.
 * @returns {Object} the formatted item
 */
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
  // Return any other item as JSON
  return JSON.stringify(item);
};

/**
 * Outputs the items to the console.
 * @param {...Object} item An item to output.
 * @protected
 */
Logger.prototype._print = function (items) {
  if (!Logger.isCSV)
    console.error.apply(console, items);
  else {
    csv([items], {eof: false}, function (err, output) {
      console.error(err || output);
    });
  }
};

/**
 * Disables this logger.
 */
Logger.prototype.disable = function () {
  this.log = _.noop;
};

/**
 * Disables all loggers or specific levels.
 * @param {...String} levels The log levels to disable
 */
Logger.disable = function () {
  if (arguments.length === 0)
    // Disable all loggers
    Logger.prototype.log = _.noop;
  else
    // Disable loggers of specific levels
    for (var i = 0; i < arguments.length; i++)
      Logger.prototype[('' + arguments[i]).toLowerCase()] = _.noop;
};

Logger.isCSV = false;

module.exports = Logger;
