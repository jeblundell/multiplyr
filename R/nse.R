# Workarounds for non-standard evaluation

#' Sort data frame
#' @param .data Data frame
#' @param ... Variables to sort by
#' @param .dots Workaround for non-standard evaluation
#' @export
arrange <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots(...)
    arrange_ (.self, .dots=.dots)
}

#' Define new variables
#' @param .data Data frame
#' @param ... Names of new variables
#' @param .dots Workaround for non-standard evaluation
#' @export
define <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    define_ (.self, .dots=.dots)
}

#' Select unique rows or unique combinations of variables
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
distinct <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    distinct_ (.data, .dots=.dots)
}

#' Faster form of usual filter function, but has restrictions
#' @param .data Data frame
#' @param ... Filtering expressions
#' @param .dots Workaround for non-standard evaluation
#' @export
fast_filter <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    fast_filter_ (.data, .dots=.dots)
}

#' Filter data
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
filter <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    filter_ (.data, .dots=.dots)
}

#' Group data
#' @param .self Data frame
#' @param ... Variables to sort by
#' @param .dots Workaround for non-standard evaluation
#' @export
group_by <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    group_by_ (.self, .dots=.dots)
}

#' Change values of existing variables (and create new ones)
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
mutate <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    mutate_ (.data, .dots=.dots)
}

#' Partition data so that each group is wholly on a node
#' @param .data Data frame
#' @param ... Names of grouping variables
#' @export
partition_group <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    partition_group_ (.data, .dots=.dots)
}

#' Rename variables
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
rename <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    rename_ (.data, .dots=.dots)
}

#' Retain only specified variables
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
select <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    select_ (.data, .dots=.dots)
}

#' Summarise data
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
summarise <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    summarise_ (.data, .dots=.dots)
}

#' Change variables and drop all others
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
transmute <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    transmute_ (.data, .dots=.dots)
}

#' Delete variables
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
undefine <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    undefine_ (.data, .dots=.dots)
}

#' @describeIn undefine
#' @export
unselect <- undefine
