
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
fast_filter <- function (.data, ...) {
    .dots <- lazyeval::lazy_dots (...)
    filtercol <- match (".filter", attr(.data, "colnames"))
    res <- ff_mwhich(.data, .dots)

    #FIXME: parallel
    #FIXME: more elegant version?

    .data[[1]][res, filtercol] <-
        .data[[1]][res, filtercol] & 1

    antires <- setdiff (1:nrow(.data[[1]]), res)
    .data[[1]][antires, filtercol] <- 0
}

.filter_rewrite <- function (lazyobj) {
    op <- lazyobj$expr[1]
    if (op == quote(`<`())) {
        op <- quote (`ff_lt`())
    } else if (op == quote(`<=`())) {
        op <- quote (`ff_le`())
    } else if (op == quote(`>`())) {
        op <- quote (`ff_gt`())
    } else if (op == quote(`>=`())) {
        op <- quote (`ff_ge`())
    } else if (op == quote(`==`())) {
        op <- quote (`ff_eq`())
    } else if (op == quote(`!=`())) {
        op <- quote (`ff_neq`())
    } else {
        stop ("Operator not supported for fast_filter")
    }
    lazyobj$expr[1] <- op
    return (lazyobj)
}
ff_lt <- function (cmp, expr) {
    return (list(as.character(as.name(substitute(cmp))),
                 c("gt", "lt"),
                 c(-Inf, expr)))
}
ff_le <- function (cmp, expr) {
    return (list(as.character(as.name(substitute(cmp))),
                 c("gt", "le"),
                 c(-Inf, expr)))
}
ff_gt <- function (cmp, expr) {
    return (list(as.character(as.name(substitute(cmp))),
                 c("gt", "lt"),
                 c(expr, Inf)))
}
ff_ge <- function (cmp, expr) {
    return (list(as.character(as.name(substitute(cmp))),
                 c("ge", "lt"),
                 c(expr, Inf)))
}
ff_neq <- function (cmp, expr) {
    return (list(as.character(as.name(substitute(cmp))),
                 c("neq"),
                 c(expr)))
}
ff_eq <- function (cmp, expr) {
    return (list(as.character(as.name(substitute(cmp))),
                 c("eq"),
                 c(expr)))
}
ff_mwhich <- function (x, lazyobj) {
    lazyobj <- lapply (lazyobj, .filter_rewrite) #FIXME: eval in x
    class (lazyobj) <- "lazy_dots"
    l <- lazyeval::lazy_eval (lazyobj)

    coln <- do.call (c, lapply (l, function (x) { x[[1]] }))
    cols <- match (coln, attr(x, "colnames"))
    comps <- lapply (l, function (x) { x[[2]] })
    vals  <- lapply (l, function (x) { x[[3]] })

    bigmemory::mwhich (x[[1]],
            cols = cols,
            vals = vals,
            comps = comps,
            op = "AND")
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
