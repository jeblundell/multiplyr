
#' @export
cldo <- function (.data, ...) {
    clusterEvalQ (attr(.data, "cl"), ...)
}

#' @export
clget_ <- function (.data, .dots) {
    parallel::clusterExport (attr(.data, "cl"), ".dots", envir=environment())
    parallel::clusterEvalQ (attr(.data, "cl"), lazy_eval (.dots, as.environment(.local)))
}

#' @export
clget <- function (.data, ...) {
    dots <- lazyeval::lazy_dots(...)
    return (clget_ (.data, dots))
}
