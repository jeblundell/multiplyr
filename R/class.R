# fastdf class functions

setOldClass (c("cluster", "SOCKcluster"))
#' Create new parallel data frame
#' @param cl Cluster object, number of nodes or NULL (default)
#' @param alloc Allocate additional space
#' @import methods
#' @exportClass Multiplyr
#' @export Multiplyr
Multiplyr <- setRefClass("Multiplyr",
    fields=list(bm              = "big.matrix",
                desc            = "big.matrix.descriptor",
                cls             = "SOCKcluster",
                factor.cols     = "numeric",
                factor.levels   = "list",
                type.cols       = "numeric",
                order.cols      = "numeric",
                pad             = "numeric",
                col.names        = "character",
                nsa             = "logical",
                grouped         = "logical",
                group           = "numeric",
                group_partition = "logical"
                ),
    methods=list(
initialize = function (..., alloc=1, cl=NULL) {
    vars <- list(...)

    if (length(vars) == 1) {
        if (is.data.frame(vars[[1]])) {
            vars <- unclass(vars[[1]])
        }
    }

    if (is.null (cl)) {
        cls <<- parallel::makeCluster (parallel::detectCores() - 1)
    } else if (is.numeric(cl)) {
        cls <<- parallel::makeCluster(cl)
    } else {
        Rdsm::mgrinit (cl)
        cls <<- cl
    }

    cluster_eval ({
        library (multiplyr)
        library (lazyeval)
    })

    special <- c(".filter", ".group")
    nrows <- length(vars[[1]])
    ncols <- length(vars) + alloc + length(special)
    col.names <<- c(names(vars), rep(NA, alloc), special)
    order.cols <<- c(seq_len(length(vars)), rep(0, alloc), rep(0, length(special)))
    Rdsm::mgrmakevar(cls, ".bm", nr=nrows, nc=ncols)
    bm <<- .bm

    bm[,match(".filter", col.names)] <<- 1

    type.cols <<- rep(0, ncols)

    for (i in 1:length(vars)) {
        tmp <- vars[[i]][1]
        if (is.numeric (tmp)) {
            bm[,i] <<- vars[[i]]
        } else if (is.factor (tmp)) {
            factor.cols <<- c(factor.cols, i)
            factor.levels <<- append(factor.levels, list(levels(vars[[i]])))
            bm[,i] <<- as.numeric(vars[[i]])
            type.cols[i] <<- 1
        } else if (is.character (tmp)) {
            factor.cols <<- c(factor.cols, i)
            vars[[i]] <- as.factor(vars[[i]])
            factor.levels <<- append(factor.levels, list(levels(vars[[i]])))
            bm[,i] <<- as.numeric(vars[[i]])
            type.cols[i] <<- 2
        }
    }
    pad <<- rep(0, ncols)
    for (i in seq_len(length(factor.cols))) {
        pad[factor.cols[i]] <<- max(nchar(factor.levels[[i]]))
    }

    nsa <<- FALSE
    grouped <<- FALSE
    group <<- 0
    group_partition <<- FALSE

    desc <<- bigmemory.sri::describe (.bm)
    .master <- .self
    cluster_export (".master")
    cluster_eval ({
        .master$reattach()
        NULL
    })
#     return (.master %>% partition())
},
reattach = function (descriptor = desc) {
    bm <<- bigmemory::attach.big.matrix(descriptor)
},
cluster_export = function (var, envir=parent.frame()) {
    parallel::clusterExport (cls, var, envir)
},
cluster_eval = function (...) {
    parallel::clusterEvalQ (cls, ...)
},
show = function (max.row=10) {
    if (is.null(max.row) || max.row == 0 || max.row > nrow(bm)) {
        max.row <- nrow(bm)
    }

    cols <- seq_len(length(order.cols))[order(order.cols)]
    cols <- cols[order.cols[order(order.cols)] > 0]

    cat (sprintf ("\n    Multiplyr data frame\n\n"))
    pc <- pad
    for (i in seq_len(ncol(bm))[pc==0 & order.cols > 0]) {
        pc[i] <- max(nchar(as.character(bm[1:max.row,i])))
    }

    out <- ""
    for (i in cols) {
        out <- .p(out,
                  sprintf(.p("%",pc[i],"s "),
                          col.names[i]))
    }
    cat (.p(out, "\n"))

    rows <- bigmemory::mwhich (bm,
                               cols=match(".filter", col.names),
                               vals=1,
                               comps=list("eq"))
    rows.avail <- length(rows)
    if (is.null(max.row) || max.row == 0) {
        max.row <- nrow(bm)
    }
    if (rows.avail > max.row) {
        rows <- rows[1:max.row]
    }

    for (i in rows) {
        out <- ""
        for (j in cols) {
            v <- bm[i,j]
            if (type.cols[j] > 0) {
                f <- match (j, factor.cols)
                v <- factor.levels[[f]][v]
            }
            out <- .p(out,
                      sprintf(.p("%",pc[j],"s "),
                              v))
        }
        cat (.p(out, "\n"))
    }
    if (rows.avail > max.row) {
        cat (sprintf ("\n... %d of %d rows omitted ...\n",
                      rows.avail - max.row,
                      rows.avail))
    }
    cat ("\n")
},
get = function (i=NULL, j=NULL) {
    if (is.null(i)) {
        rowslice <- NULL
    } else {
        rowslice <- i
    }
    if (is.null(j)) {
        j <- which (order.cols > 0)
        cols <- col.names[j]
    } else {
        cols <- j
    }

    m <- match (cols, col.names)
    filtercol <- match (".filter", col.names)
    filtered <- bm[, filtercol] == 1
    if (!is.null(rowslice)) {
        filtered[-rowslice] <- FALSE
    }
    out <- NULL
    for (i in m) {
        itype <- type.cols[i]
        if (itype == 0) {
            o <- bm[,i]
        } else if (itype == 1) {
            f <- match (i, factor.cols)
            o <- factor(bm[,i],
                         levels=seq_len(length(factor.levels[[f]])),
                         labels=factor.levels[[f]])
        } else if (itype == 2) {
            f <- match (i, factor.cols)
            o <- factor.levels[[f]][bm[,i]]
        }
        o <- o[filtered]
        if (is.null(out)) {
            out <- o
        } else {
            out <- cbind (data.frame(out), data.frame(o))
        }
    }
    if (length(m) > 1) {
        names (out) <- col.names[m]
    }
    return (out)
}
))

#' @export
as.fastdf <- function (x, cl=NULL) {
    UseMethod ("as.fastdf")
}

#' @export
as.fastdf.data.frame <- function (x, cl=NULL) {
    return (fastdf (x))
}

#' @export
as.data.frame.fastdf <- function (x) {
    ncols <- ncol(x[[1]])
    nrows <- nrow(x[[1]])
    value <- vector("list", ncols)

    for (i in seq_len(ncols)[attr(x, "type.cols") == 0]) { #numeric
        value[[i]] <- x[[1]][,i]
    }
    for (i in seq_len(ncols)[attr(x, "type.cols") == 1]) { #factor
        f <- match(i, attr(x, "factor.cols"))
        value[[i]] <- factor(x[[1]][,i],
                             levels=seq_len(length(attr(x, "factor.levels")[[f]])),
                             labels=attr(x, "factor.levels")[[f]])
    }
    for (i in seq_len(ncols)[attr(x, "type.cols") == 2]) { #character
        f <- match(i, attr(x, "factor.cols"))
        value[[i]] <- attr(x, "factor.levels")[[f]][x[[1]][,i]]
    }

    names(value) <- attr(x, "colnames")
    attr(value, "row.names") <- .set_row_names(nrows)
    attr(value, "class") <- "data.frame"
    value
}

#' @export
`$<-.fastdf` <- function (x, var, value) {
    x[, var] <- value
    return (x)
}

#' @export
`[<-.fastdf` <- function (x, i, j, value) {
    if (nargs() == 4) {
        missing.i <- missing(i)
        missing.j <- missing(j)
    } else {
        j <- i
        missing.i <- TRUE
    }
    len <- length(value)

    if (!is.numeric(j)) {
        j <- match (j, attr(x, "colnames"))
    }

    if (missing.i) {
        # [, j] <-
        # [j] <-
        if (len == 1 || len == nrow(x[[1]])) {
            x[[1]][, j] <- factor_map (x, j, value)
        } else {
            stop (sprintf("replacement data has %d rows to replace %d", len, nrow(x[[1]])))
        }
    } else if (missing.j) {
        # [i, ] <-
        stop ("FIXME: row replace")
    } else {
        # [i, j] <-
        stop ("FIXME: row/col replace")
    }
    x
}

#' @export
`names<-.fastdf` <- function (x, value) {
    attr(x, "colnames") <- value
}

#' @export
names.fastdf <- function (x) {
    attr(x, "colnames")
}

#' @export
with.fastdf <- function (data, expr, ...) {
    eval (substitute(expr), as.environment.fastdf(data), enclos = parent.frame())
}

#' @export
as.environment.fastdf <- function (x) {
    # Intended use: attr(dat, "bindenv") <- as.environment(dat)
    if (!"bindenv" %in% names(attributes(x))) {
        bindenv <- new.env()
    } else {
        bindenv <- attr(x, "bindenv")
    }
    bindenv <- bind_variables (x, bindenv)

    return (bindenv)
}

#' @export
sort.fastdf <- function (x, decreasing = FALSE, ...) {
    dots <- lazyeval::lazy_dots (...)
    .sort.fastdf (x, decreasing, dots)
}
