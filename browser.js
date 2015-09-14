// Make the ldf-client module available as a global browser object
window.ldf = require('./ldf-client');
window.N3  = require('./node_modules/n3'); // expose the same N3 version as used in the client
