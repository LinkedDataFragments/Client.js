
var MetadataExtractor = require('./MetadataExtractor'),
    rdf = require('../util/RdfUtil');

// Dummy class to check if the hydra:freeTextQuery is present in the metadata
function FreeTextExtractor(options) {
  if (!(this instanceof FreeTextExtractor))
    return new FreeTextExtractor(options);
  MetadataExtractor.call(this);
}
MetadataExtractor.inherits(FreeTextExtractor);

FreeTextExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  tripleStream.on('end', sendMetadata);
  tripleStream.on('data', extractFreeText);

  // Tries to extract count information from the triple
  function extractFreeText(triple) {
    // TODO: cheating
    if (triple.object === rdf.HYDRA + 'freetextQuery') {
      sendMetadata(true);
    }
  }
  // Sends the metadata through the callback and disables further extraction
  function sendMetadata(metadata) {
    tripleStream.removeListener('end', sendMetadata);
    tripleStream.removeListener('data', extractFreeText);
    callback(null, metadata || {});
  }
};

module.exports = FreeTextExtractor;
