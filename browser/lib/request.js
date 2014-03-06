/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser replacement for the request module. */

// Requests the given resource, returning the response through the callback
function request(settings, callback) {
  // Delegate the request to jQuery's AJAX module
  var jqXHR = jQuery.ajax({
    url: settings.url,
    timeout: settings.timeout,
    type: settings.method || 'GET',
    headers: { accept: 'text/turtle' },
  })
  .done(function () { sendResult(); })
  .fail(function (jqXHR, textStatus, error) {
    // Don't consider 404 a breaking error
    return sendResult(jqXHR.status === 404 ? null : error);
  });

  function sendResult(error) {
    var headers = { 'content-type': jqXHR.getResponseHeader('content-type') };
    callback(null, { statusCode: jqXHR.status, headers: headers }, jqXHR.responseText);
  }
}

module.exports = request;
