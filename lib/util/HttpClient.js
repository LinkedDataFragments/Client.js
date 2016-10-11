/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */

var TransformIterator = require('asynciterator').TransformIterator,
    logger = require('../util/Logger.js'),
    _ = require('lodash'),
    parseLink = require('parse-link-header');

/**
 * Creates a new `HttpClient`.
 * @classdesc An `HttpClient` retrieves representations of resources using HTTP
 * and provides access to them through an iterator interface.
 * It performs request pooling and time-based content negotiation.
 * @param {String} [options.request] The HTTP request module to use
 * @param {String} [options.contentType=* / *] The desired content type of representations
 * @param {integer} [options.concurrentRequests=10] Maximum number of concurrent requests per client
 * @constructor
 */
function HttpClient(options) {
  if (!(this instanceof HttpClient))
    return new HttpClient(options);

  // Initialize options
  options = options || {};
  this._request = options.request || require('./Request');
  this._defaultHeaders = _.pick({
    'accept': options.contentType || '*/*',
    'accept-encoding': 'gzip,deflate',
    'accept-datetime': options.datetime && options.datetime.toUTCString(),
  }, _.identity);
  this._logger = options.logger || logger('HttpClient');
  this._maxActiveRequestCount = options.concurrentRequests || 10;

  // Set up request queue
  this._requestId = 0;
  this._queued = [];
  this._active = {};
  this._activeCount = 0;
}

/**
 * Retrieves a representation of the resource with the given URL.
 * @param {string} url The URL of the resource
 * @param {Object} [headers] Additional HTTP headers to add
 * @param {Object} [options] Additional options for the HTTP request
 * @returns {AsyncIterator} An iterator of the representation
 */
HttpClient.prototype.get = function (url, headers, options) {
  return this.request(url, 'GET', headers, options);
};

/**
 * Retrieves a representation of the resource with the given URL.
 * @param {string} url The URL of the resource
 * @param {string} [method='GET'] method The HTTP method to use
 * @param {Object} [headers] Additional HTTP headers to add
 * @param {Object} [options] Additional options for the HTTP request
 * @returns {AsyncIterator} An iterator of the representation
 */
HttpClient.prototype.request = function (url, method, headers, options) {
  var request = _.assign({
    id: this._requestId++,
    startTime: new Date(),
    url: url,
    method: method || 'GET',
    headers: _.assign({}, this._defaultHeaders, headers),
    timeout: 5000,
    followRedirect: true,
    // maximize buffer size to drain the response stream, since unconsumed responses
    // can lead to out-of-memory errors (http://nodejs.org/api/http.html)
    response: new TransformIterator({ maxBufferSize: Infinity }),
  }, options);

  // Queue the request and start it when possible
  this._queued.push(request);
  this._startNextRequest();

  return request.response;
};

// Starts the next queued request when possible
HttpClient.prototype._startNextRequest = function (previousRequest) {
  // Remove a possible previous request from the list of active requests
  if (previousRequest && delete this._active[previousRequest.id])
    this._activeCount--;
  // Try to start the next request
  if (this._activeCount < this._maxActiveRequestCount && this._queued.length)
    this._startRequest(this._queued.shift());
};

// Performs the given request
HttpClient.prototype._startRequest = function (request) {
  // Initiate the actual HTTP request
  var httpRequest, self = this;
  this._logger.info('Requesting', request.url);
  try { httpRequest = this._request(request); }
  catch (error) { return setImmediate(handleRequestError, error); }

  // Mark the request as active
  this._activeCount++;
  this._active[request.id] = request;

  // Process the HTTP response
  httpRequest.on('response', function (httpResponse) {
    // Immediately start working on the next request
    self._startNextRequest(request);

    // Did we ask for a time-negotiated response, but haven't received one?
    if (request.headers['accept-datetime'] && !httpResponse.headers['memento-datetime']) {
      // The links might have a timegate that can help us
      var links = httpResponse.headers.link && parseLink(httpResponse.headers.link);
      if (links && links.timegate) {
        // Respond with a time-negotiated response from the timegate instead
        var timegateResponse = self.request(links.timegate.url,
                                            request.method, request.headers);
        request.response.source = timegateResponse;
        request.response.copyProperties(timegateResponse,
                                        ['statusCode', 'contentType', 'responseTime']);
        return;
      }
    }

    // Emit the response and its metadata
    request.response.source = httpResponse;
    request.response.setProperties({
      statusCode: httpResponse.statusCode,
      contentType: (httpResponse.headers['content-type'] || '').replace(/\s*(?:;.*)?$/, ''),
      responseTime: new Date() - request.startTime,
    });
  });

  // In case of error, move the next request and emit the error on the response
  httpRequest.on('error', handleRequestError);
  function handleRequestError(error) {
    self._startNextRequest(request);
    if (!request.aborted && error.code !== 'ETIMEDOUT')
      request.response.emit('error', error);
  }

  // Aborts the request
  request.abort = function () {
    if (!request.aborted) {
      request.aborted = true;
      try { httpRequest.abort(); }
      catch (error) { /* ignore */ }
      self._startNextRequest(request);
    }
  };
};

/** Aborts all active and pending requests. */
HttpClient.prototype.abortAll = function () {
  this._queued = [];
  for (var id in this._active)
    this._active[id].abort();
};

module.exports = HttpClient;
