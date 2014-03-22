/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** An TriplesIterator converts Turtle into triples. */

var TransformIterator = require('./Iterator').TransformIterator,
    N3 = require('n3');

// Creates an iterator from an array
function TriplesIterator(turtleIterator, options) {
  if (!(this instanceof TriplesIterator))
    return new TriplesIterator(turtleIterator);
  TransformIterator.call(this, turtleIterator);

  // Capture the output from the N3 parser
  var self = this, parser = this._parser = new N3.Parser(options);
  parser.parse(function (error, triple) {
    triple && self._push(triple) ||
    error  && self.emit('error', error);
  });
  this.on('end', parser.end.bind(parser));
}
TransformIterator.inherits(TriplesIterator);

// Sends a chunk of Turtle to the N3 parser
TriplesIterator.prototype._transform = function (source, push) {
  var chunk = source.read();
  chunk && this._parser.addChunk(chunk);
};

module.exports = TriplesIterator;
