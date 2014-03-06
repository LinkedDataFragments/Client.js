/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser version of LinkedDataFragmentHtmlParser */

// Creates a new LinkedDataFragmentHtmlParser
function LinkedDataFragmentHtmlParser() { }

LinkedDataFragmentHtmlParser.prototype = {
  // Parses the specified Linked Data Fragment
  parse: function (document, documentUrl) {
    throw new Error('RDFa parsing in the browser not implemented yet.');
  },
};

module.exports = LinkedDataFragmentHtmlParser;
