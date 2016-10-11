/*! @license MIT ©2014-2016 Ruben Verborgh and Miel Vander Sande, Ghent University - imec */
/* loosely based on https://github.com/visionmedia/log.js/blob/master/lib/log.js */
/* eslint no-console: 0 */

var rdf = require('./RdfUtil'),
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
  // Stringify falsy items
  if (!item)
    return '' + item;
  // Format an array
  if (item instanceof Array)
    return JSON.stringify(item.map(this._format, this));
  // Format a triple
  if (item.subject && item.predicate && item.object)
    return rdf.toQuickString(item);
  // Return any other item as JSON
  return typeof item === 'string' ? item : JSON.stringify(item);
};

/**
 * Outputs the items to the console.
 * @param {Array} items The items to output.
 * @protected
 */
Logger.prototype._print = function (items) {
  console.error.apply(console, items);
};

/**
 * Outputs the items to the console as CSV.
 * @param {Array} items The items to output.
 * @protected
 */
Logger.prototype._printCSV = function (items) {
  console.error(items.map(function (item) {
    return !/["\n\r,]/.test(item) ? item : '"' + item.replace(/"/g, '""') + '"';
  }).join(','));
};

/**
 * Disables all loggers below the specified level.
 * @param {String} level The log level to disable
 */
Logger.setLevel = function (level) {
  var levelIndex = _.indexOf(LOG_LEVELS, level.toUpperCase());
  if (levelIndex >= 0) {
    while (++levelIndex < LOG_LEVELS.length)
      Logger.prototype[LOG_LEVELS[levelIndex].toLowerCase()] = _.noop;
  }
};

/**
 * Returns whether the given logging level is enabled
 * @param {String} level The log level to check
 */
Logger.enabled = function (level) {
  return (level in Logger.prototype) && (Logger.prototype[level] !== _.noop);
};

/**
 * Sets the logging mode of all loggers.
 * @param {String} modeName The name of the mode ("CSV", "plain")
 */
Logger.setMode = function (modeName) {
  if (modeName.toUpperCase() === 'CSV')
    Logger.prototype._print = Logger.prototype._printCSV;
};

module.exports = Logger;
