/* An NQuadsFragmentParser parses Quad Pattern Fragment in N-Quads. */

var TurtleFragmentParser = require('./TurtleFragmentParser');

// Creates a new NQuadsFragmentParser
function NQuadsFragmentParser(source, fragmentUrl) {
  if (!(this instanceof NQuadsFragmentParser))
    return new NQuadsFragmentParser(source, fragmentUrl);
  TurtleFragmentParser.call(this, source, fragmentUrl);
}
TurtleFragmentParser.inherits(NQuadsFragmentParser);

NQuadsFragmentParser.prototype._fields = TurtleFragmentParser.prototype._fields.concat(['context']);

// Indicates whether the class supports the content type
NQuadsFragmentParser.supportsContentType = function (contentType) {
  return (/^(?:application\/n-quads)$/).test(contentType);
};

module.exports = NQuadsFragmentParser;