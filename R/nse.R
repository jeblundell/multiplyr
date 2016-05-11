# Workarounds for non-standard evaluation

#' Sort data
#' @param .self Data frame
#' @param ... Variables to sort by
#' @param .dots Workaround for non-standard evaluation
#' @export
arrange <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots(...)
    arrange_ (.self, .dots=.dots)
}

#' Define new variables
#' @param .self Data frame
#' @param ... Names of new variables
#' @param .dots Workaround for non-standard evaluation
#' @export
define <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    define_ (.self, .dots=.dots)
}

#' Select unique rows or unique combinations of variables
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @param auto_compact Compact data after operation
#' @export
distinct <- function (.self, ..., auto_compact = NULL) {
    .dots <- lazyeval::lazy_dots (...)
    distinct_ (.self, .dots=.dots, auto_compact=auto_compact)
}

#' Filter data
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @param auto_compact Compact data after operation
#' @export
filter <- function (.self, ..., auto_compact=NULL) {
    .dots <- lazyeval::lazy_dots (...)
    filter_ (.self, .dots=.dots, auto_compact=auto_compact)
}

#' Group data
#' @param .self Data frame
#' @param ... Variables to sort by
#' @param .cols Columns to group by (used internally)
#' @param .dots Workaround for non-standard evaluation
#' @param auto_partition Re-partition across cluster after operation
#' @export
group_by <- function (.self, ..., auto_partition=NULL) {
    .dots <- lazyeval::lazy_dots (...)
    group_by_ (.self, .dots=.dots, auto_partition=auto_partition)
}

#' Change values of existing variables (and create new ones)
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
mutate <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    mutate_ (.self, .dots=.dots)
}

#' Partition data so that each group is wholly on a node
#' @param .self Data frame
#' @param ... Names of grouping variables
#' @param .dots Workaround for non-standard evaluation
#' @export
partition_group <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    partition_group_ (.self, .dots=.dots)
}

#' Summarise data locally with reduction
#' @describeIn summarise
#' @export
reduce <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    reduce_ (.self, .dots=.dots)
}

#' Rename variables
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
rename <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    rename_ (.self, .dots=.dots)
}

#' Retain only specified variables
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
select <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    select_ (.self, .dots=.dots)
}

#' Summarise data
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @param auto_compact Compact data after operation
#' @export
summarise <- function (.self, ..., auto_compact=auto_compact) {
    .dots <- lazyeval::lazy_dots (...)
    summarise_ (.self, .dots=.dots, auto_compact=auto_compact)
}

#' Change variables and drop all others
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
transmute <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    transmute_ (.self, .dots=.dots)
}

#' Delete variables
#' @param .self Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
undefine <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    undefine_ (.self, .dots=.dots)
}

#' @describeIn undefine
#' @export
unselect <- undefine
