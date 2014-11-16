/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A FragmentsClient fetches Triple Pattern Fragments through HTTP. */

var HttpClient = require('../util/HttpClient'),
    Iterator = require('../iterators/Iterator'),
    rdf = require('../util/RdfUtil'),
    Cache = require('lru-cache'),
    _ = require('lodash');

var parserTypes = [
  require('./TurtleFragmentParser'),
  require('./NQuadsFragmentParser'),
];
var accept = 'application/n-quads;q=1.0,text/turtle;q=0.9,application/n-triples;q=0.7,text/n3;q=0.6';

// Creates a new FragmentsClient
function FragmentsClient(startFragment, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(startFragment, options);

  options = _.defaults(options || {}, { contentType: accept });
  this._cache = new Cache({ max: 100 });
  this._client = options.httpClient || new HttpClient(options);
  if (typeof startFragment !== 'string') {
    this._startFragment = startFragment;
  }
  else {
    this._startFragmentUrl = startFragment;
    (this._startFragment = new Fragment(this._client)).loadFromUrl(startFragment);
  }
}

// Returns the Triple Pattern Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  // Check whether the fragment was cached
  var cache = this._cache, key = JSON.stringify(pattern);
  if (cache.has(key))
    return cache.get(key).clone();

  // Create a dummy iterator until the fragment is loaded
  var fragment = new Fragment(this._client, this._startFragmentUrl);
  this._startFragment.getProperty('controls', function (controls) {
    // Replace all variables and blanks in the pattern by `null`
    var subject   = rdf.isVariableOrBlank(pattern.subject)   ? null : pattern.subject;
    var predicate = rdf.isVariableOrBlank(pattern.predicate) ? null : pattern.predicate;
    var object    = rdf.isVariableOrBlank(pattern.object)    ? null : pattern.object;
    var context   = rdf.isVariableOrBlank(pattern.context)   ? null : pattern.context;

    // Only attempt to fetch the fragment if its components are valid
    if (rdf.isLiteral(subject) || rdf.isLiteral(predicate)) return fragment.empty();

    // Load and cache the fragment
    pattern = { subject: subject, predicate: predicate, object: object, context: context };
    fragment.loadFromUrl(controls.getFragmentUrl(pattern));
  });
  cache.set(key, fragment);
  return fragment.clone();
};

// Creates a new Triple Pattern Fragment
function Fragment(httpClient, startFragmentUrl) {
  if (!(this instanceof Fragment))
    return new Fragment(httpClient, startFragmentUrl);
  Iterator.call(this);

  this._client = httpClient;
  this._startFragmentUrl = startFragmentUrl;
}
Iterator.inherits(Fragment);

// Reads data from the current page of the fragment
Fragment.prototype._read = function () {
  if (this._fragmentPage) {
    var item = this._fragmentPage.read();
    item && this._push(item);
  }
};

// Loads the Triple Pattern Fragment located at the given URL
Fragment.prototype.loadFromUrl = function (pageUrl) {
  // Fetch the page of the fragment
  var headers = { 'user-agent': 'Triple Pattern Fragments Client' };
  if (this._startFragmentUrl) headers.referer = this._startFragmentUrl;
  var fragmentPage = this._client.get(pageUrl, headers), self = this;
  // Don't parse the fragment if its retrieval was unsuccessful
  fragmentPage.getProperty('statusCode', function (statusCode) {
    if (statusCode !== 200)
      fragmentPage.emit('error', new Error('Status code: ' + statusCode));
  });
  // When the content type is known, find a compatible parser
  fragmentPage.getProperty('contentType', function (contentType) {
    var hasParser = parserTypes.some(function (Parser) {
      if (Parser.supportsContentType(contentType)) {
        // Parse the fetched page of the fragment
        var parsedPage = self._fragmentPage = new Parser(fragmentPage, pageUrl);
        parsedPage.on('readable', function () { self.emit('readable'); });

        // When the page ends, try to load the next page
        parsedPage.on('end', function () {
          // Find the next page's URL through hypermedia controls in the current page
          var controls = parsedPage.getProperty('controls'), nextPage;
          try { nextPage = controls && controls.nextPage; }
          catch (controlError) {}
          // If no next page is found, this fragment has ended
          if (!nextPage) return self._end();
          // Otherwise, load the next page
          self.loadFromUrl(nextPage);
        });
        // Pass errors to the page
        parsedPage.on('error', function (error) { fragmentPage.emit('error', error); });

        // Pass the metadata and controls of the page to the fragment
        if (!self.getProperty('metadata'))
          parsedPage.getProperty('metadata', self.setProperty.bind(self, 'metadata'));
        if (!self.getProperty('controls'))
          parsedPage.getProperty('controls', self.setProperty.bind(self, 'controls'));

        // A new page of data has been loaded, so this fragment is readable again
        return self.emit('readable'), true;
      }
    });
    if (!hasParser)
      self.emit('error', new Error('No parser for ' + contentType + ' at ' + pageUrl));
  });
  // If an error occurs, assume the fragment page is empty
  fragmentPage.on('error', function () {
    fragmentPage.close();
    self.empty();
  });
};

// Empties the fragment and returns it
Fragment.prototype.empty = function () {
  return this.single(null);
};

// Adds one single triple to the fragment and returns it
Fragment.prototype.single = function (triple) {
  if (!this.getProperty('metadata'))
    this.setProperty('metadata', { totalTriples: triple ? 1 : 0 });
  triple && this._push(triple);
  this._end();
  return this;
};

module.exports = FragmentsClient;
