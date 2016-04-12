.dots2names <- function (x, dots) {
    as.vector (sapply (dots, function (x) { as.character (x$expr) }))
}

.sort.fastdf <- function (x, decreasing=FALSE, dots) {
    #FIXME: parallel version
    namelist <- .dots2names (x, dots)
    cols <- match(namelist, attr(x, "colnames"))
    bigmemory::mpermute (x[[1]], cols=cols)
}

#' @export
sort.fastdf <- function (x, decreasing = FALSE, ...) {
    dots <- lazyeval::lazy_dots (...)
    .sort.fastdf (x, decreasing, dots)
}
