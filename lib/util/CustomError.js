/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */

var SKIP_INITIALIZATION = {};

// Creates a constructor for an Error with the given name
function createErrorType(name, BaseError, init) {
  function E(message, skip) {
    this.message = message;
    if (!Error.captureStackTrace)
      this.stack = (new Error()).stack;
    else
      Error.captureStackTrace(this, this.constructor);
    if (init && skip !== SKIP_INITIALIZATION)
      init.apply(this, arguments);
  }
  BaseError = BaseError || Error;
  E.prototype = BaseError === Error ?
                new Error() : new BaseError('', SKIP_INITIALIZATION);
  E.prototype.name = name;
  E.prototype.constructor = E;
  E.prototype.super = BaseError;
  return E;
}

module.exports = createErrorType;
