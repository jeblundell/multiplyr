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

#' @export
group_by <- function (.data, ...) {
    dots <- lazyeval::lazy_dots (...)
    namelist <- as.vector (sapply (dots, function (x) { as.character (x$expr) }))
    cols <- match(namelist, attr(.data, "colnames"))
    Gcol <- match(".sort", attr(.data, "colnames"))

    #FIXME: parallel version

    bigmemory::mpermute (.data[[1]], cols=cols)
    sm1 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=1, lastRow=nrow(.data[[1]])-1)
    sm2 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=2, lastRow=nrow(.data[[1]]))
    if (length(cols) == 1) {
        breaks <- which (sm1[,cols] != sm2[,cols])
    } else {
        breaks <- which (!apply (sm1[,cols] == sm2[,cols], 1, all))
    }
    breaks <- c(breaks, nrow(.data[[1]]))
    last <- 0
    for (i in 1:length(breaks)) {
        .data[[1]][(last+1):breaks[i],Gcol] <- i
        last <- breaks[i]
    }
    rm (sm1, sm2)
}
