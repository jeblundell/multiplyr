multiplyr provides a simple interface for manipulating data combined with easy parallel processing capabilities. It's intended that this works very similarly (eventually almost interchangably) with the dplyr package, as many people may be familiar with that already.

'''r \# Create a new data frame with 2 variables: x and G dat \<- Multiply (x=1:100, G=rep(c("A", "B", "C", "D"), each=25))

Group data (A, B, C, D)
=======================

dat %\>% group\_by (G)

Create a new variable (y) with random data, the same length as x
================================================================

dat %\>% mutate (y=rnorm(length(x)))

Remove any rows where y \< 0
============================

dat %\>% filter (y\<0)

Summarise to give 4 rows (A, B, C, D), with number of rows in each group
========================================================================

dat %\>% summarise (N=length(x)) '''

See [Multiplyr basics](vignettes/basics.Rmd)

Roadmap
-------

### 0.1.1

-   Bugfixes
-   Optimisations
-   Minor dplyr functions not already implemented

### 0.2

-   Non-local cluster nodes
-   Remote read.csv
-   Matrix operations (+/- GPU)
-   SQL
-   Joins