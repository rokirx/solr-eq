# Elevation Query Extension

In many cases the correct search behaviour cannot be achieved by relevancy calculations only. It is sometimes easier to formulate a search requirement by describing the relevant document sets using more than one query and then placing this document sets  in a defined order one after the other.

This project implements an extension to Solr which enables to use subqueries for placing subsets of hits in the desired order in the final result set.

# Problem description

Assume we have a list of queries

> `Q_0, Q_1,..., Q_n`

We would like to compose the result list consisting of the results of individual queries:


|Expected results|
|-|
|Documents retrieved by `Q1`|
|Documents retrieved by `Q2`|
|...|
|Documents retrieved by `Qn`|

We may want to limit the results of some subqueries and also to sort different subqueries differently.

The new search behaviour is implemented as a Lucene query, which can handle the required hit sorting. To trigger this behaviour on demand we introduce a new operator `<<` along with some query parser extensions to formulate the queries. If the new operator is not used, the behaviour of Solr is not changed in any way.
