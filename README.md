# Linked Data Fragments Client
<img src="http://linkeddatafragments.org/images/logo.svg" width="200" align="right" alt="" />

[![Build Status](https://travis-ci.org/LinkedDataFragments/Client.js.svg?branch=master)](https://travis-ci.org/LinkedDataFragments/Client.js)
[![npm version](https://badge.fury.io/js/ldf-client.svg)](https://www.npmjs.com/package/ldf-client)
[![Docker Automated Build](https://img.shields.io/docker/automated/linkeddatafragments/client.js.svg)](https://hub.docker.com/r/linkeddatafragments/client.js/)

On today's Web, Linked Data is published in different ways,
including [data dumps](http://downloads.dbpedia.org/3.9/en/),
[subject pages](http://dbpedia.org/page/Linked_data),
and [results of SPARQL queries](http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=CONSTRUCT+%7B+%3Fp+a+dbpedia-owl%3AArtist+%7D%0D%0AWHERE+%7B+%3Fp+a+dbpedia-owl%3AArtist+%7D&format=text%2Fturtle).
We call each such part a [**Linked Data Fragment**](http://linkeddatafragments.org/) of the dataset.

The issue with the current Linked Data Fragments
is that they are either so powerful that their servers suffer from low availability rates
([as is the case with SPARQL](http://sw.deri.org/~aidanh/docs/epmonitorISWC.pdf)),
or either don't allow efficient querying.

Instead, this client solves queries by accessing **Triple Pattern Fragments**.
<br>
Each Triple Pattern Fragment offers:

- **data** that corresponds to a _triple pattern_
  _([example](http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3ARestaurant))_.
- **metadata** that consists of the (approximate) total triple count
  _([example](http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=))_.
- **controls** that lead to all other fragments of the same dataset
  _([example](http://data.linkeddatafragments.org/dbpedia?subject=&predicate=&object=%22John%22%40en))_.


## Execute SPARQL queries

You can execute SPARQL queries against Triple Pattern Fragments like this:
```bash
$ ldf-client http://fragments.dbpedia.org/2015/en query.sparql
```
The arguments to the `ldf-client` command are:

0. Any fragment of the dataset you want to query, in this case DBpedia.
[_More datasets._](http://linkeddatafragments.org/data/)
0. A file with the query you want to execute (this can also be a string).


### From within your application

First, create a `FragmentsClient` to fetch fragments of a certain dataset.
<br>
Then create a `SparqlIterator` to evaluate SPARQL queries on that dataset.

```JavaScript
var ldf = require('ldf-client');
var fragmentsClient = new ldf.FragmentsClient('http://fragments.dbpedia.org/2015/en');

var query = 'SELECT * { ?s ?p <http://dbpedia.org/resource/Belgium>. ?s ?p ?o } LIMIT 100',
    results = new ldf.SparqlIterator(query, { fragmentsClient: fragmentsClient });
results.on('data', function (result) { console.log(result); });
```


## Install the client

The command-line client requires [Node.js](http://nodejs.org/) 4.0 or higher
and is tested on OSX and Linux.
To install, execute:
```bash
$ [sudo] npm install -g ldf-client
```

### Browser version

The client can also run in Web browsers via [browserify](https://github.com/substack/node-browserify), which provides browser equivalents for Node.js-specific parts.
[Try the live demo.](http://client.linkeddatafragments.org/)

To build a browserified version, run:
```
npm install [-g] browserify
npm run browserify
```
The browserified version will be written to `ldf-client-browser.js`.

The API is the same as that of the Node version, except that `ldf = require('ldf-client')` is no longer necessary, since `ldf` is exposed as `window.ldf`.

### From source
To install from the latest GitHub sources, execute:
```bash
$ git clone git@github.com:LinkedDataFragments/Client.js
$ cd Client.js
$ npm install .
```

Then run the application with:
```bash
$ bin/ldf-client http://fragments.dbpedia.org/2015/en queries/artists-york.sparql
```
The `queries` folder contains several example queries for DBpedia.


### _(Optional)_ Running in a Docker container

If you want to rapidly deploy use the client as a microservice, you can build a [Docker](https://www.docker.com/) container as follows:

```bash
$ docker build -t ldf-client .
```
After that, you can run your newly created container:
```bash
$ docker run -it --rm ldf-client http://fragments.dbpedia.org/2015/en 'SELECT * WHERE { ?s ?p ?o } LIMIT 100'
```
Mounting custom config and query files can be done like this:
```bash
$ docker run -it --rm $(pwd)/config.json:/tmp/config.json $(pwd)/query.sparql:/tmp/query.sparql ldf-client http://fragments.dbpedia.org/2015/en -f /tmp/query.sparql -c /tmp/config.json
```

## License
The Linked Data Fragments client is written by [Ruben Verborgh](http://ruben.verborgh.org/) and colleagues.

This code is copyrighted by [Ghent University – imec](http://idlab.ugent.be/)
and released under the [MIT license](http://opensource.org/licenses/MIT).
