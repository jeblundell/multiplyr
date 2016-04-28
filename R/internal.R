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
.dots2names <- function (x, dots) {
    as.vector (sapply (dots, function (x) { as.character (x$expr) }))
}

#' @export
#' @keywords internal
#' @rdname internal
.sort.fastdf <- function (x, decreasing=FALSE, dots=NULL, cols=NULL, with.group=FALSE) {
    if (is.null(cols)) {
        namelist <- .dots2names (x, dots)
        cols <- match(namelist, attr(x, "colnames"))
    }
    if (with.group) {
        Gcol <- match(".group", attr(x, "colnames"))
        if (Gcol %in% cols) {
            cols <- cols[cols != Gcol]
        }
        cols <- c(Gcol, cols)
    }
    bigmemory::mpermute (x[[1]], cols=cols)
    x
}

#' @export
#' @keywords internal
#' @rdname internal
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
#' @keywords internal
#' @rdname internal
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
#' @keywords internal
#' @rdname internal
ff_lt <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("gt", "lt"), c(-Inf, res)))
}

#' @export
#' @keywords internal
#' @rdname internal
ff_le <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("gt", "le"), c(-Inf, res)))
}

#' @export
#' @keywords internal
#' @rdname internal
ff_gt <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("gt", "lt"), c(res, Inf)))
}

#' @export
#' @keywords internal
#' @rdname internal
ff_ge <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("ge", "lt"), c(res, Inf)))
}

#' @export
#' @keywords internal
#' @rdname internal
ff_neq <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("neq"),c(res)))
}

#' @export
#' @keywords internal
#' @rdname internal
ff_eq <- function (cmp, expr) {
    var <- as.character(as.name(substitute(cmp)))
    res <- ff_expr(var, expr)
    return (list(var, c("eq"), c(res)))
}

#' @export
#' @keywords internal
#' @rdname internal
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
#' @keywords internal
#' @rdname internal
alloc_col <- function (x, name=".tmp") {
    res <- which (is.na (attr(x, "colnames")))
    if (length(res) == 0) {
        stop ("No free columns available")
    } else {
        attr(x, "colnames")[res[1]] <- name
        attr(x, "type.cols")[res[1]] <- 0
        attr(x, "order.cols")[res[1]] <- max(attr(x, "order.cols"))+1
        return (x)
    }
}

#' @export
#' @keywords internal
#' @rdname internal
free_col <- function (x, col) {
    attr(x, "colnames")[col] <- NA
    attr(x, "type.cols")[col] <- 0
    attr(x, "order.cols")[col] <- 0
    return (x)
}

#' @export
#' @keywords internal
#' @rdname internal
.partition_all <- function (.data, max.row = nrow(.data[[1]])) {
    cl <- attr(.data, "cl")
    N <- length(cl)

    nr <- distribute (max.row, N)
    if (max.row < N) {
        nr[nr != 0] <- 1:max.row
        for (i in 1:N) {
            .first <- .last <- nr[i]
            parallel::clusterExport (cl[i], c(".first", ".last"), envir=environment())
        }
    } else {
        last <- cumsum(nr)
        first <- c(0, last)[1:N] + 1
        for (i in 1:N) {
            .first <- first[i]
            .last <- last[i]
            parallel::clusterExport (cl[i], c(".first", ".last"), envir=environment())
        }
    }
    parallel::clusterEvalQ (cl, {
        .empty <- (.last < .first || .last == 0)
        if (!exists(".local")) {
            .local <- .master
        }
        if (.empty) { return (NULL) }
        .local[[1]] <- sub.big.matrix(.master[[1]],
                                      firstRow=.first,
                                      lastRow=.last)
        NULL
    })
    return(.data)
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
.rebuild_grouped <- function (.data) {
    parallel::clusterEvalQ (attr(.data, "cl"), {
        if (.empty) { return(NULL) }
        if (length(.groups) == 0) { return(NULL) }

        .grouped <- list()
        for (.g in 1:length(.groups)) {
            .grp <- group_restrict(.local, .groups[.g])
            attr(.grp, "bindenv") <- as.environment (.grp)
            .grouped <- append(.grouped, list(.grp))
        }

        NULL
    })
    .data
}

#' @export
#' @keywords internal
#' @rdname internal
.filter_rows <- function (x, tmpcol, filtercol, rows) {
    x[[1]][, tmpcol] <- 0
    x[[1]][rows, tmpcol] <- 1

    x[[1]][, filtercol] <- x[[1]][, filtercol] *
        x[[1]][, tmpcol]
}

#' @export
#' @keywords internal
#' @rdname internal
.filter_vector <- function (x, filtercol, rows) {
    x[[1]][, filtercol] <- x[[1]][, filtercol] * rows
}

#' @export
#' @keywords internal
#' @rdname internal
.filter_range <- function (x, filtercol, start, end) {
    x[[1]][, filtercol] <- 0
    x[[1]][start:end, filtercol] <- 1
}

