
#' @export
arrange <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots(...)
    arrange_ (.data, .dots=.dots)
}

#' @export
clget <- function (.data, ...) {
    dots <- lazyeval::lazy_dots(...)
    return (clget_ (.data, .dots=dots))
}

#' @export
define <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    define_ (.data, .dots=.dots)
}

#' @export
distinct <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    distinct_ (.data, .dots=.dots)
}

#' @export
fast_filter <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    fast_filter_ (.data, .dots=.dots)
}

#' @export
filter <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    filter_ (.data, .dots=.dots)
}

#' @export
group_by <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    group_by_ (.data, .dots=.dots)
}

#' @export
mutate <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    mutate_ (.data, .dots=.dots)
}

#' @export
rename <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    rename_ (.data, .dots=.dots)
}

#' @export
select <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    select_ (.data, .dots=.dots)
}

#' @export
transmute <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    transmute_ (.data, .dots=.dots)
}

#' @export
undefine <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    undefine_ (.data, .dots=.dots)
}

#' @export
ungroup <- function (.data) {
    attr (.data, "grouped") <- FALSE
    parallel::clusterEvalQ (attr(.data, "cl"), attr(.local, "grouped") <- FALSE)
    return (.data)
}

#' @export
unselect <- undefine
