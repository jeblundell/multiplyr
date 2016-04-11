
#' @export
sort.fastdf <- function (x, decreasing = FALSE, ...) {
    dots <- lazyeval::lazy_dots (...)
    namelist <- as.vector (sapply (dots, function (x) { as.character (x$expr) }))
    cols <- match(namelist, attr(x, "colnames"))
    sortcol <- match(".sort", attr(x, "colnames"))

    #FIXME: parallel version

    #Is there an issue storing the sort order in the matrix itself?
    x[[1]][,sortcol] <- bigmemory::morder (x[[1]], cols=cols, decreasing=decreasing)
    bigmemory::mpermute (x[[1]], order = x[[1]][,sortcol])
}
