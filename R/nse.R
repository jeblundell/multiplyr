# Workarounds for non-standard evaluation

#' Sort data
#'
#' Sorts data frame using the columns specified
#'
#' This function takes a parallel data frame and sorts it by the first
#' column specified. If there are any ties then it sorts by the second,
#' and so on. Use \code{desc(x)} to specify that x should be sorted in
#' descending order (see \code{\link{desc}}).
#'
#' @template nse
#' @family row manipulations
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(1:4, each=25))
#' dat %>% arrange (x)
#' dat %>% arrange (G, x)
#' dat %>% shutdown()
#' }
arrange <- function (.self, ...) {
    .dots <- dotscapture (...)
    arrange_ (.self, .dots=.dots)
}

#' Define new columns
#'
#' Takes a list of column names and creates them. Optionally uses a template to
#' copy across factor/character mappings.
#'
#' This function is used to create new columns in a data frame. Specifying
#' just the name will create a new numeric column. If specified in the form
#' of \code{var=template}, then a new column named \code{var} will be
#' created using \code{template} as a template. This is useful for creating
#' new factors.
#'
#' @template nse
#' @family column manipulations
#' @export
define <- function (.self, ...) {
    .dots <- dotscapture (...)
    define_ (.self, .dots=.dots)
}

#' Select unique rows or unique combinations of variables
#'
#' Select unique rows or unique combinations of variables
#'
#' When called with no additional parameters, \code{distinct()}, this
#' function will filter out any non-unique rows in the specified data frame.
#' Specifying column names will limit the uniqueness checks to only those
#' columns, i.e. \code{distinct(G)} will limit the data frame to only
#' have unique values of G.
#'
#' Note that if data are grouped, then this will find unique rows or
#' combinations for each group.
#'
#' @template nse
#' @family row manipulations
#' @param auto_compact Compact data after operation
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25))
#' dat %>% distinct(G)
#' dat %>% shutdown()
#' }
distinct <- function (.self, ..., auto_compact = NULL) {
    .dots <- dotscapture (...)
    distinct_ (.self, .dots=.dots, auto_compact=auto_compact)
}

#' Filter data
#'
#' Select a subset of rows whose columns meet a set of criteria
#'
#' This may be used to only include rows that match a particular set of
#' criteria. For example, \code{filter(x>10)} would remove any rows from
#' the data whose value in the x column is not greater than 10. Multiple
#' filtering criteria may be combined with \code{&} or \code{|}
#' (representing "and", "or").
#'
#' @template nse
#' @family row manipulations
#' @param auto_compact Compact data after operation
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, y=rnorm(100))
#' dat %>% filter (x<50 & y>0)
#' dat %>% shutdown()
#' }
filter <- function (.self, ..., auto_compact=NULL) {
    .dots <- dotscapture (...)
    filter_ (.self, .dots=.dots, auto_compact=auto_compact)
}

#' Group data
#'
#' Groups data by specified columns: further operations then work within those groups
#'
#' Many data analysis problems require working with particular combinations
#' of data. For example, finding the average sales for a given day of the
#' week could be achieved with \code{group_by(day)} and
#' \code{summarise(sales = mean(sales)}. This would result in a data frame
#' with 7 rows (1 for each group) with the average sales stored in the
#' sales column.
#'
#' Multiple grouping variables may be specified, separated by columns. The
#' above example could be extended to group by month as well as weekday,
#' e.g. \code{group_by(month, day)}. The resulting data frame would then
#' have 12 blocks of 7 (84 rows) with an average for each week day in that
#' month provided the same way as above.
#'
#' @template nse
#' @family row manipulations
#' @param .cols Columns to group by (used internally)
#' @param auto_partition Re-partition across cluster after operation
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25))
#' dat %>% group_by (G) %>% summarise (N=length(x))
#' dat %>% shutdown()
#' }
group_by <- function (.self, ..., auto_partition=NULL) {
    .dots <- dotscapture (...)
    group_by_ (.self, .dots=.dots, auto_partition=auto_partition)
}

#' Change values of existing variables (and create new ones)
#'
#' This function is used to alter the data frame, without dropping any columns (unlike \code{transmute}, which drops any columns not explicitly specified)
#'
#' @template nse
#' @family data manipulations
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100)
#' dat %>% mutate (x=x*2)
#' dat %>% shutdown()
#' }
mutate <- function (.self, ...) {
    .dots <- dotscapture (...)
    mutate_ (.self, .dots=.dots)
}

#' Partition data so that each group is wholly on a node
#'
#' Partitions data across the cluster such that each group is wholly
#' contained on a single node.
#'
#' This should not typically be called explicitly; group_by achieves the same
#' thing. Generally speaking it would be fairly pointless to group things and
#' then not have each group fully accessible, but theoretically is possible to
#' so (use \code{group_by (..., auto_partition=FALSE}).
#'
#' @family cluster functions
#' @template nse
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25))
#' dat %>% partition_group (G)
#' dat %>% shutdown()
#' }
partition_group <- function (.self, ...) {
    .dots <- dotscapture (...)
    partition_group_ (.self, .dots=.dots)
}

#' Summarise data (with local reduction)
#'
#' \code{summarise} is used to summarise data on each node: \code{reduce} is then used to ensure that there's one overall summary
#'
#' When data have not been grouped, calling \code{summary(...)} will result
#' in each node summarising the data it has available. This means that if
#' there are 3 nodes in the cluster, then there will be 3 summary values.
#' \code{reduce} is used to bring all those together to a single value.
#'
#' @template nse
#' @family data manipulations
#' @param auto_compact Compact data after operation
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x = 1:100)
#' dat %>% summarise (N = length(x))
#' dat %>% shutdown()
#'
#' dat <- Multiplyr (x = 1:100)
#' dat %>% summarise (N = length(x)) %>% reduce(N = sum(N))
#' dat %>% shutdown()
#' }
reduce <- function (.self, ..., auto_compact=NULL) {
    .dots <- dotscapture (...)
    reduce_ (.self, .dots=.dots, auto_compact=auto_compact)
}

#' Rename variables
#'
#' Takes a list of newname=oldname pairs and renames columns
#'
#' @template nse
#' @family column manipulations
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x = 1:100)
#' dat %>% rename(y=x)
#' dat["y"]
#' dat %>% shutdown()
#' }
rename <- function (.self, ...) {
    .dots <- dotscapture (...)
    rename_ (.self, .dots=.dots)
}

#' Retain only specified variables
#'
#' Takes a list of columns and returns a data frame with only those columns and in the order specified
#'
#' @template nse
#' @family column manipulations
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x = 1:100, y = 100:1, z = rep(123, 100))
#' dat %>% select (y, x)
#' dat %>% shutdown()
#' }
select <- function (.self, ...) {
    .dots <- dotscapture (...)
    select_ (.self, .dots=.dots)
}

#' Summarise data
#'
#' Produces a summary statistic for each group or cluster node (the latter case should then be followed up with \code{reduce})
#'
#' @template nse
#' @family data manipulations
#' @param auto_compact Compact data after operation
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25))
#' dat %>% group_by (G) %>% summarise (xbar = mean(x))
#' dat %>% shutdown()
#' }
summarise <- function (.self, ..., auto_compact=NULL) {
    .dots <- dotscapture (...)
    summarise_ (.self, .dots=.dots, auto_compact=auto_compact)
}

#' Change variables and drop all others
#'
#' This function works like a combination of \code{mutate} and \code{select}: it may be used to modify values in a data frame, and then drops any column not explicitly specified
#'
#' @template nse
#' @family data manipulations
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, y=100:1)
#' dat %>% mutate (x=x*2)
#' dat %>% shutdown()
#' }
transmute <- function (.self, ...) {
    .dots <- dotscapture (...)
    transmute_ (.self, .dots=.dots)
}

#' Delete variables
#'
#' In much the same way that \code{define} creates new columns, \code{undefine} will delete them
#'
#' @template nse
#' @family column manipulations
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, y=100:1)
#' dat %>% undefine (y)
#' dat %>% shutdown()
#' }
undefine <- function (.self, ...) {
    .dots <- dotscapture (...)
    undefine_ (.self, .dots=.dots)
}

#' @rdname undefine
#' @export
unselect <- undefine
