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
distinct <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    distinct_ (.self, .dots=.dots)
}

#' Filter data
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
filter <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    filter_ (.self, .dots=.dots)
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
mutate <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    mutate_ (.self, .dots=.dots)
}

#' Partition data so that each group is wholly on a node
#' @param .self Data frame
#' @param ... Names of grouping variables
#' @export
partition_group <- function (.self, ...) {
    .dots <- lazyeval::lazy_dots (...)
    partition_group_ (.self, .dots=.dots)
}

#' Rename variables
#' @param .data Data frame
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
