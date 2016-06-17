/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser replacement for the request module using jQuery. */

var EventEmitter = require('events').EventEmitter,
    SingleIterator = require('../../lib/iterators/Iterator').SingleIterator,
    _ = require('lodash');

require('setimmediate');

// Headers we cannot send (see https://www.w3.org/TR/XMLHttpRequest/#the-setrequestheader()-method)
var UNSAFE_REQUEST_HEADERS = ['accept-encoding', 'user-agent', 'referer'];
// Headers we need to obtain
var RESPONSE_HEADERS = ['content-type', 'link', 'memento-datetime'];

// Creates an HTTP request with the given settings
function createRequest(settings) {
  var request = new EventEmitter();

  // Delegate the request to jQuery's AJAX module
  var jqXHR = jQuery.ajax({
    url: settings.url,
    timeout: settings.timeout,
    type: settings.method,
    headers: _.omit(settings.headers, UNSAFE_REQUEST_HEADERS),
  })
  // Emit the result as a readable response iterator
  .success(function () {
    var response = new SingleIterator(jqXHR.responseText || '');
    response.statusCode = jqXHR.status;
    response.headers = _.object(RESPONSE_HEADERS, RESPONSE_HEADERS.map(jqXHR.getResponseHeader));
    request.emit('response', response);
  })
  // Emit an error if the request fails
  .fail(function () {
    request.emit('error', new Error('Error requesting ' + settings.url));
  });
  // Aborts the request
  request.abort = function () { jqXHR.abort(); };

  return request;
}

module.exports = createRequest;
