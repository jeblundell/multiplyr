# Internal functions not really intended for public use

#' Internal functions
#' @name internal
#' @keywords internal
NULL

#' @export
#' @keywords internal
#' @rdname internal
.p <- function (...) { paste (..., sep="") }

#' @export
#' @keywords internal
#' @rdname internal
.dots2names <- function (dots) {
    nm <- names (dots)
    exprs <- nm == ""
    if (any(exprs)) {
        nm[exprs] <- as.vector (sapply (dots[[exprs]], function (x) { as.character (x$expr) }))
    }
    return (nm)
}

#' @export
#' @keywords internal
#' @rdname internal
no.strings.attached <- function (x) {
    attr(x, "nsa") <- TRUE
    x
}

#' @export
#' @keywords internal
#' @rdname internal
.filter_range <- function (x, filtercol, start, end) {
    x[[1]][, filtercol] <- 0
    x[[1]][start:end, filtercol] <- 1
}

