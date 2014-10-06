var FragmentsClient = require('../FragmentsClient'),
		Iterator = require('../../iterators/Iterator'),
    rdf = require('../../util/RdfUtil'),
    _ = require('lodash');


function FederatedFragmentsClient(startFragments, options) {
  if (!(this instanceof FederatedFragmentsClient))
    return new FederatedFragmentsClient(startFragments, options);

	this._clients = _.each(startFragments || [], function (startFragment) {
		return new FragmentsClient(startFragment, options);
	});
}

FederatedFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
	function isBoundPatternOf(child, parent) {
    return (rdf.isVariable(parent.subject) || (parent.subject === child.subject)) &&
           (rdf.isVariable(parent.predicate) || (parent.predicate === child.predicate)) &&
           (rdf.isVariable(parent.object) || (parent.object === child.object));
	}
	
	var compoundFragment = new CompoundFragment();
	
	_.each(this._clients, function (client) {
		var fragment = client.getFragmentByPattern(pattern);
		
		fragment.getProperty('metadata', function (metadata) {
		
      var count = metadata.totalTriples;
      compoundFragment.getProperty('metadata', function (metadata) {
        metadata.totalTriples = metadata.totalTriples + count; // extend current weighted sum

        compoundFragment.setProperty('metadata', metadata);
      });

		});

		compoundFragment.register(fragment);
	
	});
	
	return compoundFragment;
};


// Creates a new compound Triple Pattern Fragment
function CompoundFragment() {
  if (!(this instanceof CompoundFragment))
    return new CompoundFragment();
  Iterator.call(this);

  this._fragments = [];
}
Iterator.inherits(CompoundFragment);

CompoundFragment.prototype.register = function (fragment) {
	this._fragments.push(fragment);
};

CompoundFragment.prototype._read = function () {
	var fragments = this._fragments;
	if (!fragments.length)
		return;

	var fragment = fragments.pop(), item = fragment.read();
	
	if (item === null) {
		(!fragment.ended) && fragment.once('readable', function () {
			fragments.push(fragment);
		});
		
		this._read();
	} else
		this._push(item);
};


module.exports = FederatedFragmentsClient;