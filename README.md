# Linked Data Fragments Client <img src="http://linkeddatafragments.org/images/logo.svg" width="100" align="right" alt="" />
On today's Web, Linked Data is published in different ways,
which include [data dumps](http://downloads.dbpedia.org/3.9/en/),
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

## Installation

This client requires [Node.js](http://nodejs.org/) 0.10 or higher
and is tested on OSX and Linux.
To install, execute:
```bash
$ [sudo] npm install -g ldf-client
```

## Launching queries through the standalone application

You can execute SPARQL queries as follows:
```bash
$ ldf-client query.sparql
```
Here, `query.sparql` contains your query;
alternatively, you can pass the query as a string.

By default, the LDF server [data.linkeddatafragments.org](http://data.linkeddatafragments.org/) is used,
but you can specify your own by creating your own `config.json` based on `config-default.json`:
```bash
$ ldf-client config.json query.sparql
```

## Using the library

First, set up a client that will fetch fragments from a certain source.
<br>
You can then use this client to evaluate SPARQL queries.

```JavaScript
var ldf = require('ldf-client');

var fragmentsClient = new ldf.FragmentsClient('http://data.linkeddatafragments.org/dbpedia');

var query = 'SELECT * { ?s ?p <http://dbpedia.org/resource/Belgium>. } LIMIT 100'
var results = new ldf.SparqlIterator(query, { fragmentsClient: fragmentsClient });
results.on('data', console.log);
```

### Browser version

The client can also run in Web browsers via [browserify](https://github.com/substack/node-browserify).
[Live demo.](http://client.linkeddatafragments.org/)

The API is the same as that of the Node version.
<br>
A usage example is available in [a separate project](https://github.com/LinkedDataFragments/WebClient).

## License
The Linked Data Fragments client is written by [Ruben Verborgh](http://ruben.verborgh.org/).

This code is copyrighted by [Multimedia Lab – iMinds – Ghent University](http://mmlab.be/)
and released under the [MIT license](http://opensource.org/licenses/MIT).
