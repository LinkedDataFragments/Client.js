/*! @license ©2013-2015 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Main ldf-client module exports. */

// Replace local `require` by a lazy loader,
// so we can keep `require` calls for static analyzers such as browserify
var globalRequire = require;
require = function (path) { return function () { return require(path); } };

// Temporarily set lazy initializers as exports
var exports = module.exports = {
  SparqlIterator: require('./lib/triple-pattern-fragments/SparqlIterator.js'),
  FragmentsClient: require('./lib/triple-pattern-fragments/federated/FederatedFragmentsClient'),
  Logger: require('./lib/util/Logger'),
<<<<<<< HEAD
  SparqlResultWriter: SparqlResultWriter,
  Iterator: require('./lib/iterators/Iterator.js'),
  MultiTransformIterator: require('./lib/iterators/MultiTransformIterator.js'),
  Util: require('./lib/util/RdfUtil.js'),
  HttpClient: require('./lib/util/HttpClient'),
  MetadataExtractor: require('./lib/extractors/MetadataExtractor'),
  CompositeExtractor: require('./lib/extractors/CompositeExtractor'),
  ControlsExtractor: require('./lib/extractors/ControlsExtractor'),
  FragmentsClient: require('./lib/triple-pattern-fragments/FragmentsClient'),
  TurtleFragmentIterator: require('./lib/triple-pattern-fragments/TurtleFragmentIterator.js')
=======
  HttpClient: require('./lib/util/HttpClient'),
  SparqlResultWriter: function () {
    var SparqlResultWriter = require('./lib/writers/SparqlResultWriter');
    SparqlResultWriter.register('application/json', './JSONResultWriter');
    SparqlResultWriter.register('application/sparql-results+json', './SparqlJSONResultWriter');
    SparqlResultWriter.register('application/sparql-results+xml', './SparqlXMLResultWriter');
    return SparqlResultWriter;
  },
>>>>>>> master
};

// Replace exports by properties that load on demand
Object.keys(exports).forEach(function (submodule) {
  var loadSubmodule = exports[submodule];
  Object.defineProperty(exports, submodule, {
    configurable: true,
    enumerable: true,
    get: function () {
      // Replace the (currently executing) lazy property handler by the actual module
      delete exports[submodule];
      return exports[submodule] = loadSubmodule();
    },
  });
});

// Restore original require
require = globalRequire;
