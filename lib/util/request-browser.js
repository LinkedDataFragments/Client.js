/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser replacement for the request module using jQuery. */

var EventEmitter = require('events').EventEmitter,
    SingleIterator = require('../../lib/iterators/Iterator').SingleIterator;

require('setimmediate');

// Requests the given resource as an iterator
function createRequest(settings) {
  var request = new EventEmitter();

  // Delegate the request to jQuery's AJAX module
  var jqXHR = jQuery.ajax({
    url: settings.url,
    timeout: settings.timeout,
    type: settings.method,
    headers: { accept: 'application/n-quads;q=1.0,text/turtle;q=0.9,application/n-triples;q=0.7,text/n3;q=0.6' },
  })
  // Emit the result as a readable response iterator
  .always(function () {
    if (jqXHR.failure) return;
    var response = new SingleIterator(jqXHR.responseText || '');
    response.statusCode = jqXHR.status;
    response.headers = { 'content-type': jqXHR.getResponseHeader('content-type') };
    request.emit('response', response);
  });

  return request;
}

module.exports = createRequest;
