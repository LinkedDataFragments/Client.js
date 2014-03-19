/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
var N3 = require('n3'),
    util = module.exports = N3.Util({});

/* Methods for URIs */

// Checks whether two URIs are equal after decoding, to make up for encoding differences
util.decodedURIEquals = function (URIa, URIb) {
  if (URIa === URIb) return true;
  try { return decodeURI(URIa) === decodeURI(URIb); }
  catch (error) { return false; }
};


/* Methods for triples */

// Creates a triple object from the components
util.triple = function (subject, predicate, object) {
  return { subject: subject, predicate: predicate, object: object };
};


/* Methods for variables and triple patterns */

// Indicates whether the entity represents a variable
util.isVariable = function (entity) {
  return !entity || /^urn:var#|^\?/.test(entity);
};

// Indicates whether the entity represents a variable or blank node
util.isVariableOrBlank = function (entity) {
  return !entity || /^urn:var#|^\?|^_:/.test(entity);
};

// Creates a filter for triples that match the given pattern
util.tripleFilter = function (triplePattern) {
  var pattern = triplePattern || {},
      subject   = util.isVariableOrBlank(pattern.subject)   ? null : pattern.subject,
      predicate = util.isVariableOrBlank(pattern.predicate) ? null : pattern.predicate,
      object    = util.isVariableOrBlank(pattern.object)    ? null : pattern.object;
  return function (triple) {
    return (subject === null   || subject   === triple.subject) &&
           (predicate === null || predicate === triple.predicate) &&
           (object === null    || object    === triple.object);
  };
};

// Apply the given bindings to the triple pattern, returning a bound copy
util.applyBindings = function (bindings, triplePattern) {
  return {
    subject:   bindings[triplePattern.subject]   || triplePattern.subject,
    predicate: bindings[triplePattern.predicate] || triplePattern.predicate,
    object:    bindings[triplePattern.object]    || triplePattern.object,
  };
};

// Find the bindings that transform the pattern into the triple
util.findBindings = function (triplePattern, boundTriple) {
  return util.extendBindings(null, triplePattern, boundTriple);
};

// Create extended bindings to include bindings that transform the pattern into the triple
util.extendBindings = function (bindings, triplePattern, boundTriple) {
  var newBindings = Object.create(null);
  for (var binding in bindings)
    newBindings[binding] = bindings[binding];
  util.addBinding(newBindings, triplePattern.subject,   boundTriple.subject);
  util.addBinding(newBindings, triplePattern.predicate, boundTriple.predicate);
  util.addBinding(newBindings, triplePattern.object,    boundTriple.object);
  return newBindings;
};

// Add a binding that binds the left component to the right
util.addBinding = function (bindings, left, right) {
  // The left side may be variable; the right side may not
  if (util.isVariableOrBlank(right))
    throw new Error('Right-hand side may not be variable.');
  // If the left one is the variable
  if (util.isVariableOrBlank(left)) {
    // Add it to the bindings if it wasn't already bound
    if (!(left in bindings))
      bindings[left] = right;
    // The right-hand side should be consistent with the binding
    else if (right !== bindings[left])
      throw new Error(['Cannot bind', left, 'to', right,
                       'because it was already bound to', bindings[left] + '.'].join(' '));
  }
  // Both are constants, so they should be equal for a successful binding
  else if (left !== right) {
    throw new Error(['Cannot bind', left, 'to', right].join(' '));
  }
  // Return the extended bindings
  return bindings;
};


/* Common RDF namespaces and URIs */

namespace('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', [
  'type', 'subject', 'predicate', 'object',
]);

namespace('var', 'urn:var#');

namespace('void', 'http://rdfs.org/ns/void#', [
  'triples',
]);

namespace('hydra', 'http://rdfs.org/ns/void#', [
  'search', 'template', 'mapping', 'property', 'variable',
]);

namespace('foaf', 'http://xmlns.com/foaf/0.1/');

namespace('dbpedia', 'http://dbpedia.org/resource/');
namespace('dbpedia-owl', 'http://dbpedia.org/ontology/');

function namespace(prefix, base, names) {
  var key = prefix.replace(/[^a-z]/g, '').toUpperCase();
  util[key] = base;
  names && names.forEach(function (name) {
    util[key + '_' + name.toUpperCase()] = base + name;
  });
}

Object.freeze(util);
