var SparqlResultWriter = require('./SparqlResultWriter'),
    TransformIterator = require('../iterators/Iterator').TransformIterator;

/**
 * Formats results as JSON
 **/
function JSONResultWriter(sparqlIterator) {
  TransformIterator.call(this, sparqlIterator);
}
SparqlResultWriter.inherits(JSONResultWriter);

JSONResultWriter.prototype._transform = function (result, done) {
  this._push(JSON.stringify(result));
  done();
};

module.exports = JSONResultWriter;
