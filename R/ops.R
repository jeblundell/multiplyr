.dots2names <- function (x, dots) {
    as.vector (sapply (dots, function (x) { as.character (x$expr) }))
}

.sort.fastdf <- function (x, decreasing=FALSE, dots) {
    namelist <- .dots2names (x, dots)
    cols <- match(namelist, attr(x, "colnames"))
    bigmemory::mpermute (x[[1]], cols=cols)
}

#' @export
alloc_col <- function (x, name=".tmp") {
    res <- match (NA, attr(x, "colnames"))
    if (length(res) == 0) {
        stop ("No free columns available")
    } else {
        attr(x, "colnames")[res[1]] <- name
        return (res[1])
    }
}

#' @export
free_col <- function (x, col) {
    attr(x, "colnames")[col] <- NA
}


#' @export
sort.fastdf <- function (x, decreasing = FALSE, ...) {
    dots <- lazyeval::lazy_dots (...)
    .sort.fastdf (x, decreasing, dots)
}

#' @export
partition <- function (.data) {
    cl <- attr(.data, "cl")
    N <- length(cl)
    nr <- rep(floor(nrow(.data[[1]])/N), N)
    nr[1] <- nr[1] + (nrow(.data[[1]])-sum(nr))
    last <- cumsum(nr)
    first <- c(0, last)[1:N] + 1
    for (i in 1:N) {
        .first <- first[i]
        .last <- last[i]
        parallel::clusterExport (cl[i], c(".first", ".last"), envir=environment())
    }
    parallel::clusterEvalQ (cl, {
        if (!exists(".local")) {
            .local <- .master
        }
        .local[[1]] <- sub.big.matrix(.master[[1]],
                                      firstRow=.first,
                                      lastRow=.last)
        NULL})
    return(.data)
}

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

    parallel::clusterExport (attr(.data, "cl"), ".dots", envir=environment())

    parallel::clusterEvalQ (attr(.data, "cl"), {
        filtercol <- match (".filter", attr(.local, "colnames"))
        tmpcol <- alloc_col (.local)
        res <- ff_mwhich(.local, .dots)

        if (length(res) == 0) {
            .local[[1]][, filtercol] <- 0
        } else {
            .local[[1]][, tmpcol] <- 0
            .local[[1]][res, tmpcol] <- 1

            .local[[1]][, filtercol] <- .local[[1]][, filtercol] &
                .local[[1]][, tmpcol]
        }
        free_col (.local, tmpcol)
        NULL
    })
    return (.data)
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
#' @export
ff_expr <- function (var, expr) {
    col <- match (var, attr(.local, "colnames"))
    type <- attr(.local, "type.cols")[col]
    if (type == 0) {
        return (expr)
    } else {
        f <- match (col, attr(.local, "factor.cols"))
        l <- match (expr, attr(.local, "factor.levels")[[f]])
        return (l)
    }
}
#' @export
ff_lt <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("gt", "lt"), c(-Inf, res)))
}
#' @export
ff_le <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("gt", "le"), c(-Inf, res)))
}
#' @export
ff_gt <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("gt", "lt"), c(res, Inf)))
}
#' @export
ff_ge <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("ge", "lt"), c(res, Inf)))
}
#' @export
ff_neq <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("neq"),c(res)))
}
#' @export
ff_eq <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("eq"), c(res)))
}
#' @export
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
    Gcol <- match(".group", attr(.data, "colnames"))

    .sort.fastdf (.data, decreasing=FALSE, dots)
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
    return (.data)
}

#' @export
distinct <- function (.data, ...) {
    dots <- lazyeval::lazy_dots (...)
    filtercol <- match(".filter", attr(.data, "colnames"))
    tmpcol <- alloc_col (.data)

    if (length(dots) > 0) {
        namelist <- .dots2names (.data, dots)
        cols <- match(namelist, attr(.data, "colnames"))
        .sort.fastdf (.data, decreasing=FALSE, dots)
    } else {
        cols <- attr(.data, "order.cols") > 0
        cols <- (1:length(cols))[cols]
        bigmemory::mpermute (.data[[1]], cols=cols)
    }

    sm1 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=1, lastRow=nrow(.data[[1]])-1)
    sm2 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=2, lastRow=nrow(.data[[1]]))
    if (length(cols) == 1) {
        breaks <- which (sm1[,cols] != sm2[,cols])
    } else {
        breaks <- which (!apply (sm1[,cols] == sm2[,cols], 1, all))
    }
    breaks <- c(0, breaks) + 1

    .data[[1]][, tmpcol] <- 0
    .data[[1]][breaks, tmpcol] <- 1
    .data[[1]][, filtercol] <- .data[[1]][, filtercol] &
        .data[[1]][, tmpcol]

    free_col (.data, tmpcol)

    .data
}

#' @export
rename <- function (.data, ...) {
    dots <- lazyeval::lazy_dots (...)

    .newnames <- names(dots)
    .oldnames <- as.vector (vapply (dots, function (x) { as.character (x$expr) }, ""))
    .match <- match(.oldnames, attr(.data, "colnames"))
    attr(.data, "colnames")[.match] <- .newnames
    parallel::clusterExport (attr(.data, "cl"), c(".oldnames",
                                                  ".newnames",
                                                  ".match"),
                             envir=environment())
    parallel::clusterEvalQ (attr(.data, "cl"), {
        attr(.master, "colnames")[.match] <- .newnames
        attr(.local, "colnames")[.match] <- .newnames
        NULL
    })
    .data
}

#' @export
select <- function (.data, ...) {
    dots <- lazyeval::lazy_dots (...)
    coln <- as.vector (vapply (dots, function (x) { as.character (x$expr) }, ""))
    cols <- match (coln, attr(.data, "colnames"))
    attr(.data, "order.cols") <- 0
    attr(.data, "order.cols")[sort(cols)] <- order(cols)
    # not propagated to .local on cluster as this is only for
    # display purposes (currently)
    .data
}

#' @export
slice <- function (.data, rows=NULL, start=NULL, end=NULL) {
    if (is.null(rows) && (is.null(start) || is.null(end))) {
        stop ("Must specify either rows or start and stop")
    } else if (!is.null(rows) && !(is.null(start) || is.null(end))) {
        stop ("Can either specify rows or start and stop; not both")
    }

    if (is.null(rows)) {
        rows <- start:end
    }

    filtercol <- match(".filter", attr(.data, "colnames"))

    filtered <- bigmemory::mwhich (.data[[1]],
                                   cols=filtercol,
                                   vals=1,
                                   comps="eq")
    rows <- filtered[rows]
    .data[[1]][, filtercol] <- 0
    .data[[1]][rows, filtercol] <- 1

    .data
}
