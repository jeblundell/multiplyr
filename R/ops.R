
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

#' @export
arrange <- function (.data, ...) {
    #This works on the presumption that factors have levels
    #sorted already

    dots <- lazyeval::lazy_dots(...)
    .sort.fastdf(.data, decreasing, dots)
}

#' @export
group_by <- function (.data, ...) {
    dots <- lazyeval::lazy_dots (...)

    namelist <- .dots2names (.data, dots)
    cols <- match(namelist, attr(.data, "colnames"))
    Gcol <- match(".sort", attr(.data, "colnames"))

    .sort.fastdf (.data, decreasing=FALSE, dots)
    #FIXME: parallel version
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
