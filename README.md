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

## Usage

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

### Browser version

The client also runs in Web browsers
(via [browserify](https://github.com/substack/node-browserify)).
[Live demo.](http://client.linkeddatafragments.org/)

To compile the Web version, execute:
```bash
$ npm run browser
```
This will compile the browser version of the script in the `browser` folder.

An [extended version of the Web client](https://github.com/LinkedDataFragments/WebClient) is available as a separate project.

## License
The Linked Data Fragments client is written by [Ruben Verborgh](http://ruben.verborgh.org/).

This code is copyrighted by [Multimedia Lab – iMinds – Ghent University](http://mmlab.be/)
and released under the [MIT license](http://opensource.org/licenses/MIT).
