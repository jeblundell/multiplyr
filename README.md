[![Build Status](https://travis-ci.org/jeblundell/multiplyr.svg?branch=develop)](https://travis-ci.org/jeblundell/multiplyr) [![](http://www.r-pkg.org/badges/version/multiplyr)](https://cran.r-project.org/package=multiplyr) [![](http://cranlogs.r-pkg.org/badges/grand-total/multiplyr)](https://cran.r-project.org/package=multiplyr)

Overview
--------

multiplyr provides a simple interface for manipulating data combined with easy parallel processing capabilities. It's intended that this works very similarly (eventually almost interchangably) with the dplyr package, as many people may be familiar with that already.

``` r
# Create a new data frame with 2 columns (x & G) and space for 2 new columns
dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25), alloc=2)

# Group data (A, B, C, D)
dat %>% group_by (G)

# Create a new variable (y) with random data, the same length as x
dat %>% mutate (y=rnorm(length(x)))

# Remove any rows where y < 0
dat %>% filter (y<0)

# Summarise to give 4 rows (A, B, C, D), with number of rows in each group
dat %>% summarise (N=length(x))
```

Run the following code once multiplyr is installed for more details:

``` r
vignette ("basics")
```

Installation
------------

Install latest version from CRAN:

``` r
install.packages ("multiplyr")
```

Development
-----------

Install latest stable development version:

``` r
# install.packages("devtools")
devtools::install_github("jeblundell/multiplyr", ref="stable", build_vignettes = TRUE)
```

### Branches

-   master: represents the version currently in CRAN
-   stable: the latest commit from develop that passes all tests
-   develop: current state of development
