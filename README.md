# social-network-graphx
A scala application built using GraphX to run some queries on tweets collected from twitter.
A graph is built the following way :
* A vertex embodies the top most common words found during a time interval.
* Two vertices A and B have a single edge if they share at least a common word among themselves.

The scripts do the following :
* Finds the number of edges where the number of words in the source vertex is strictly larger than the number of words in the destination vertex. (Uses Property Graph)
* Finds the most popular vertex. A vertex is the most popular if it has the most number of edges to its neighbors and it has the maximum number of words. (Uses Neighborhood Aggregation)
* Finds the average number of words in every neighbor of a vertex. (Uses Neighborhood Aggregation)
