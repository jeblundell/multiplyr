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

