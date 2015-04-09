/**
 * Created by joachimvh on 6/03/2015.
 */

/* Checks if the regex is simple enough to be handled by a TPF server. */

var MultiTransformIterator = require('../iterators/MultiTransformIterator'),
  rdf = require('../util/RdfUtil'),
  Iterator = require('../iterators/Iterator');

// TODO: stored versions of these iterators to re-use
function RegexIterator(parent, regexFilter, options) {
  if (!(this instanceof RegexIterator))
    return new RegexIterator(parent, regexFilter, options);
  if (!parent)
    parent = Iterator.single({});
  MultiTransformIterator.call(this, parent, options);

  this.variable = regexFilter.args[0];
  this._regex = regexFilter.args[1].substring(1, regexFilter.args[1].length - 1); // change '"regex"' to 'regex'
  this._caseInsensitive = regexFilter.args.length >= 3 && regexFilter.args[2].indexOf('i') >= 0; // TODO: what are the other possible flags?
  this._client = this._options.fragmentsClient;
  this._fragment = null;

  // check if we actually support this regex
  // TODO: can obviously be improved, maybe even split up into multiple iterators
  // TODO: check which chars need to be escaped
  if (this._regex.search(/[\\.^$|[(!?*+{]+/) >= 0)
    throw 'Unsupported regex.';
}
MultiTransformIterator.inherits(RegexIterator);

RegexIterator.prototype._createTransformer = function (bindings, options) {
  // no need to create more than 1 version of this iterator
  if (!this._fragment)
    this._fragment = this._client.getFragmentByPattern({substring: this._regex});

  return this._fragment.clone();
};

// variable not appearing in bindings: create new bindings by adding string to bindings
RegexIterator.prototype._readTransformer = function (fragment, fragmentBindings) {
  var triple;
  while (triple = fragment.read()) {
    var str = triple.object;
    // variable appears in binding: act as a filter iterator
    if (fragmentBindings[this.variable] && fragmentBindings[this.variable] !== str)
      continue;
    var newBindings = Object.create(null);
    for (var binding in fragmentBindings)
      newBindings[binding] = fragmentBindings[binding];
    newBindings[this.variable] = str;
    return newBindings;
  }
  return null;
};

RegexIterator.prototype.totalTriples = function (callback) {
  if (this._count)
    callback(this._count);
  if (!this._fragment)
    this._fragment = this._client.getFragmentByPattern({substring: this._regex});
  var self = this;
  this._fragment.getProperty('metadata', function (metadata) {
    self._count = metadata.totalTriples;
    callback(self._count);
  });
};

module.exports = RegexIterator;