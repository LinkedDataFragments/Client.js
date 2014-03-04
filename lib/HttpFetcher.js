/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A HttpFetcher downloads documents through HTTP. */

var q = require('q'),
    request, // expensive, load lazily
    zlib = require('zlib');

// Creates a new HttpFetcher
function HttpFetcher(maxParallel) {
  this._queue = [];    // Queue of request execution functions
  this._active = {};   // Hash of active requests
  this._pending = 0;   // The number of currently active requests
  this._maxParallel = maxParallel || 10; // Only execute this many requests in parallel
}

HttpFetcher.prototype = {
  // Returns a promise for the HTTP GET request's result
  get: function (url) {
    return this.request(url, 'GET');
  },

  // Repeats a request a number of times until successful, waiting in between
  repeat: function (url, repeats, delay) {
    var self = this;
    repeats = repeats || 10;
    delay = delay || 1000;
    return (function next() {
      return self.request(url)
        .catch(function (error) {
          if (--repeats === 0) throw error;
          return q.delay(delay).then(next);
        });
    })();
  },

  // Returns a promise for the HTTP request's result
  request: function (url, methodName) {
    var method = methodName || 'GET', requestId = methodName + url;
    // First check whether the request was already pending
    if (requestId in this._active)
      return this._active[requestId].result;

    // If not, prepare to make a request
    var self = this, deferred = q.defer();
    if (!request)
      request = require('request');

    // Request execution function
    function execute() {
      // Check whether the request is pending in the meantime
      if (requestId in self._active)
        return deferred.resolve(self._active[requestId].result);
      // If not, start the request
      var headers = { 'Accept': 'text/turtle;q=1.0,text/html;q=0.5', 'Accept-Encoding': 'gzip' },
          settings = { url: url, headers: headers, timeout: 5000, method: method, encoding: null },
          activeRequest = request(settings, onResponse);
      // Mark the request as active
      self._active[requestId] = { request: activeRequest, result: deferred.promise };
      self._pending++;
    }

    // Response callback
    function onResponse(error, response, body) {
      // Remove the request from the active list
      if (requestId in self._active) {
        delete self._active[requestId];
        self._pending--;
      }

      // Schedule a possible pending call
      var next = self._queue.shift();
      if (next)
        process.nextTick(next);

      // Reject if an error occurred
      if (error)
        return deferred.reject(new Error(error));
      if (response.statusCode >= 500)
        return deferred.reject(new Error('Request failed: ' + url));

      // Decompress the response if necessary
      if (response.headers['content-encoding'] === 'gzip')
        return zlib.gunzip(body, function (error, decodedBody) {
          delete response.headers['content-encoding'];
          onResponse(error, response, decodedBody);
        });

      // Return result through the deferred
      var contentType = (response.headers['content-type'] || 'text/html').split(';')[0];
      deferred.resolve({ url: url, status: response.statusCode,
                         type: contentType, body: body.toString('utf8') });
    }

    // Execute if possible, queue otherwise
    if (this._pending < this._maxParallel)
      execute();
    else
      this._queue.push(execute);

    return deferred.promise;
  },

  // Cancels all pending requests
  cancelAll: function () {
    for (var id in this._active)
      this._active[id].request.abort();
    this._active = {};
    this._queue = [];
  }
};

module.exports = HttpFetcher;
