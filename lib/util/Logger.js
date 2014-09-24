/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* Extended by Miel Vander Sande: loosely based on log.js
 * https://github.com/visionmedia/log.js/blob/master/lib/log.js */

var rdf = require('./RdfUtil'), csv = require('csv-stringify');

/**
 * Creates a new `Logger`.
 * @constructor
 * @classdesc A `Logger` provides debug output.
 * @param name The name of the logger
 */
function Logger(name) {
  if (!(this instanceof Logger))
    return new Logger(name);
  this._prefix = name || '';
}

/**
 * System is unusable.
 *
 * @type Number
 */
Logger.EMERGENCY = function () {
  return 0;
};
Logger.EMERGENCY.value = 'EMERGENCY';

/**
 * Action must be taken immediately.
 *
 * @type Number
 */

Logger.ALERT = function () {
  return 1;
};
Logger.ALERT.value = 'ALERT';

/**
 * Critical condition.
 *
 * @type Number
 */

Logger.CRITICAL = function () {
  return 2;
};
Logger.CRITICAL.value = 'CRITICAL';

/**
 * Error condition.
 *
 * @type Number
 */

Logger.ERROR = function () {
  return 3;
};
Logger.ERROR.value = 'ERROR';

/**
 * Warning condition.
 *
 * @type Number
 */

Logger.WARNING = function () {
  return 4;
};
Logger.WARNING.value = 'WARNING';

/**
 * Normal but significant condition.
 *
 * @type Number
 */

Logger.NOTICE = function () {
  return 5;
};
Logger.NOTICE.value = 'NOTICE';

/**
 * Purely informational message.
 *
 * @type Number
 */

Logger.INFO = function () {
  return 6;
};
Logger.INFO.value = 'INFO';

/**
 * Application debug messages.
 *
 * @type Number
 */

Logger.DEBUG = function () {
  return 7;
};
Logger.DEBUG.value = 'DEBUG';

Logger.prototype = {
  log : function (level, arg) {
    var items = Array.prototype.map.call(arg, this._format, this);
    items.unshift('[' + new Date() + ']', level.value, this._prefix);
    this._print(items);
  },
  /**
   * Logs an emergency message.
   * @param {...Object} item An item to log.
   */
  emergency : function () {
    this.log(Logger.EMERGENCY, arguments);
  },
  /**
   * Logs an alert message.
   * @param {...Object} item An item to log.
   */
  alert : function () {
    this.log(Logger.ALERT, arguments);
  },
  /**
   * Logs a critical message.
   * @param {...Object} item An item to log.
   */
  critical : function () {
    this.log(Logger.CRITICAL, arguments);
  },
  /**
   * Logs an error message.
   * @param {...Object} item An item to log.
   */
  error : function () {
    this.log(Logger.ERROR, arguments);
  },
  /**
   * Logs a warning message.
   * @param {...Object} item An item to log.
   */
  warning : function () {
    this.log(Logger.WARNING, arguments);
  },
  /**
   * Logs a notice message.
   * @param {...Object} item An item to log.
   */
  notice : function () {
    this.log(Logger.NOTICE, arguments);
  },
  /**
   * Logs an information message.
   * @param {...Object} item An item to log.
   */
  info : function () {
    this.log(Logger.INFO, arguments);
  },

  /**
   * Logs an information message.
   * @param {...Object} item An item to log.
   */
  debug : function () {
    this.log(Logger.DEBUG, arguments);
  },
};

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
 * Disables this particular logger
 */
Logger.prototype.disable = function () {
  this.log = function () {
  };
};

/**
 * Disables all loggers or specific levels.
 * @param {...Object} item An item to output.
 */
Logger.disable = function () {

  if (arguments.length > 0) {
    // disable specific levels loggers
    for (var i = 0; i < arguments.length; i++) {
      var levelFunctionName = arguments[i].value.toLowerCase();
      if (Logger.prototype[levelFunctionName])
        Logger.prototype[levelFunctionName] = function () {
        };
    }
  } else
    // disable all loggers
    Logger.prototype.log = function () {
    };
};

Logger.isCSV = false;

module.exports = Logger;
