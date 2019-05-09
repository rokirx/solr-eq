# Elevation Query Extension

In many cases the correct search behaviour cannot be achieved by relevancy calculations only. It is sometimes easier to formulate a search requirement by describing the relevant document sets using more than one query and then placing this document sets  in a defined order one after the other.

This project implements an extension to Solr which enables to use subqueries for placing subsets of hits in the desired order in the final result set.

# Problem description

Assume we have a list of queries

> `Q1, Q2,..., Qn`

We would like to compose the result list consisting of the results of individual queries:


|Expected results|
|-|
|Documents retrieved by `Q1`|
|Documents retrieved by `Q2`|
|...|
|Documents retrieved by `Qn`|

We may want to limit the results of some subqueries and also to sort different subqueries differently.

The new search behaviour is implemented as a Lucene query, which can handle the required hit sorting. To trigger this behaviour on demand we introduce a new operator `<<` along with some query parser extensions to formulate the queries. If the new operator is not used, the behaviour of Solr is not changed in any way.

# Specification
## Basic functionality

Given a list of queries

> `Q1,...,Qn`

it is possible to join them using the operator `<<`:

> `Q1 << Q2 << ... << Qn`

The set of documents retrieved is identical to the query

> `Q1 OR Q2 OR ...  OR Qn`

The sorting of the results is as follows:

- On the top of the hit list are the documents retrieved by the `Q1`.
- Then the documents of Q2 excluding those already retrieved by `Q1` follow.
- Then the documents of Q3 excluding those already retrieved by `Q1`, `Q2` follow.
- ...and so on.

The idea is to place the documents in the hit list according to the order of queries `Qi`.

The sorting of documents in each subsection of the hit list corresponding to the subqueries `Q1` is defined by the optional sorting parameter. This parameter is valid for all the subqueries `Q1` if set, otherwise the sorting is by relevancy.
The maximum number of queries which can be used with the operator is 31.

The operator `<<` can be only set on the upper level of the expression, it cannot be combined with other operators on the same level. It cannot be enclosed by brackets to form subqueries. In any case a syntax error (exception) is triggered.
Facets and filters can be used.

The algorithm does not change any relevancy values calculated by the Solr (or any custom) similarities.
The parser extension does not have any impact on existing Solr functionality. Clients are free to use or to ignore the operator.

### Sorting per subquery
It is possible to assign different sorting rules for each subquery. Then the resuls of a subquery Qi will be sorted according to specified sorting parameter disregarding any default or global settings. A typical example would be to sort the documents retrieved by some suqbquery by date in descending order to get the newest and thus more relevant documents on top and to keep the default sorting by relevancy for other subqueries. (details of parametrization will follow)

### Limited subqueries
It is possible to impose a limit on the maximal number of hits to retrieve for each individual subquery. If a limit M is set on a subquery Qi, then only M most relevant documents according to sort option on Qi will be retrieved. The truncated tail of the limited hit list can be optionally removed from the total hit list completely or moved to any desired location in the hit list.

### 'Golden' use cases
- In case a special document or a small set of documents identified by a query D should be forced to be on top of the hit list use the query  `D << Q`, where `Q `is the regular query.
- Individual documents identified by the queries `D1,...,Dn` can be placed in any specified order on the top of the hit list using the query `D1 << D2 << ... << Dn << Q`, where `Q` is the regular query.
- Any strict relevance requirements can be implemented using simple pattern `A << B << C <<...`, where the queries `A, B, C,...` identify document sets with descending relevancy.

