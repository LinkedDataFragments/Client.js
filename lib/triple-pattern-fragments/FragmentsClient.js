/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A FragmentsClient fetches Triple Pattern Fragments through HTTP. */

var HttpClient = require('../util/HttpClient'),
    Iterator = require('../iterators/Iterator'),
    rdf = require('../util/RdfUtil'),
    Cache = require('lru-cache'),
    CompositeExtractor = require('../extractors/CompositeExtractor'),
    CountExtractor = require('../extractors/CountExtractor'),
    ControlsExtractor = require('../extractors/ControlsExtractor'),
    _ = require('lodash');

var parserTypes = [
  require('./TurtleFragmentIterator'),
];
var DEFAULT_ACCEPT = 'text/turtle;q=1.0,application/n-triples;q=0.7,text/n3;q=0.6';

// Creates a new FragmentsClient
function FragmentsClient(startFragment, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(startFragment, options);

  // Set HTTP and cache options
  options = _.defaults(options || {}, { contentType: DEFAULT_ACCEPT });
  this._cache = new Cache({ max: 100 });
  this._httpClient = options.httpClient || new HttpClient(options);

  // Extract counts and triple pattern fragments controls by default
  this._metadataExtractor = options.metadataExtractor || new CompositeExtractor({
    metadata: [ new CountExtractor() ],
    controls: [ new ControlsExtractor() ],
  });

  // Fetch the start fragment if necessary
  if (typeof startFragment !== 'string')
    this._startFragment = startFragment;
  else {
    this._startFragmentUrl = startFragment;
    (this._startFragment = new Fragment(this)).loadFromUrl(startFragment);
  }
}

// Returns the Triple Pattern Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  // Check whether the fragment was cached
  var cache = this._cache, key = JSON.stringify(pattern);
  if (cache.has(key))
    return cache.get(key).clone();

  // Create a dummy iterator until the fragment is loaded
  var fragment = new Fragment(this);
  this._startFragment.getProperty('controls', function (controls) {
    // Replace all variables and blanks in the pattern by `null`
    var subject   = rdf.isVariableOrBlank(pattern.subject)   ? null : pattern.subject;
    var predicate = rdf.isVariableOrBlank(pattern.predicate) ? null : pattern.predicate;
    var object    = rdf.isVariableOrBlank(pattern.object)    ? null : pattern.object;

    // Only attempt to fetch the fragment if its components are valid
    if (rdf.isLiteral(subject) || rdf.isLiteral(predicate)) return fragment.empty();

    // Load and cache the fragment
    pattern = { subject: subject, predicate: predicate, object: object };
    fragment.loadFromUrl(controls.getFragmentUrl(pattern));
  });
  cache.set(key, fragment);
  return fragment.clone();
};

// Creates a new Triple Pattern Fragment
function Fragment(fragmentsClient) {
  if (!(this instanceof Fragment))
    return new Fragment(fragmentsClient);
  Iterator.call(this);

  this._fragmentsClient = fragmentsClient;
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
  var fragmentsClient = this._fragmentsClient,
      headers = { 'user-agent': 'Triple Pattern Fragments Client' };
  if (fragmentsClient._startFragmentUrl) headers.referer = fragmentsClient._startFragmentUrl;
  var fragmentPage = fragmentsClient._httpClient.get(pageUrl, headers), self = this;
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

        // Extract the metadata and controls of the current page
        var controls = {};
        fragmentsClient._metadataExtractor.extract({ fragmentUrl: pageUrl }, parsedPage.metadataStream,
        function (error, metadata) {
          // Emit all new properties
          for (var type in metadata)
            if (!self.getProperty(type))
              self.setProperty(type, metadata[type]);
          // Store the controls so we can find the next page
          controls = metadata.controls || controls;
        });

        // When the page ends, try to load the next page
        parsedPage.on('end', function () {
          // Find the next page's URL through hypermedia controls in the current page
          var nextPage;
          try { nextPage = controls && controls.nextPage; } catch (controlError) {}
          // If no next page is found, this fragment has ended
          if (!nextPage) return self._end();
          // Otherwise, load the next page
          self.loadFromUrl(nextPage);
        });
        // Pass errors to the page
        parsedPage.on('error', function (error) { fragmentPage.emit('error', error); });

        // Add response time to Fragments
        if (!self.getProperty('responseTime'))
          fragmentPage.getProperty('responseTime', self.setProperty.bind(self, 'responseTime'));

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
  // Add 1ms response time to Fragments
  if (!this.getProperty('responseTime'))
    this.setProperty('responseTime', 0);

  triple && this._push(triple);
  this._end();
  return this;
};

module.exports = FragmentsClient;
