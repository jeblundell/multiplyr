
#' Constructor
#' @param .cl Cluster object, number of nodes or NULL (default)
#' @param alloc Allocate additional space
#' @export
fastdf <- function (..., alloc=0, cl = NULL) {
    vars <- list(...)

    if (length(vars) == 1) {
        if (is.data.frame(vars[[1]])) {
            vars <- unclass(vars[[1]])
        }
    }

    if (is.null (cl)) {
        cl <- parallel::makeCluster (parallel::detectCores() - 1)
    } else if (is.numeric(cl)) {
        cl <- parallel::makeCluster(cl)
    } else {
        Rdsm::mgrinit (cl)
    }

    parallel::clusterEvalQ (cl, library(fastdf))
    parallel::clusterEvalQ (cl, library(lazyeval))

    special <- c(".sort", ".filter")
    nrows <- length(vars[[1]])
    ncols <- length(vars) + alloc + length(special)
    names.cols <- c(names(vars), rep(NA, alloc), special)
    order.cols <- c(seq_len(length(vars)), rep(0, alloc), rep(0, length(special)))
    Rdsm::mgrmakevar(cl, ".bm", nr=nrows, nc=ncols)

    .bm[,match(".filter", names.cols)] <- 1

    factor.cols <- c()
    factor.levels <- list()
    type.cols <- rep(0, ncols)

    for (i in 1:length(vars)) {
        tmp <- vars[[i]][1]
        if (is.numeric (tmp)) {
            .bm[,i] <- vars[[i]]
        } else if (is.factor (tmp)) {
            factor.cols <- c(factor.cols, i)
            factor.levels <- append(factor.levels, list(levels(vars[[i]])))
            .bm[,i] <- as.numeric(vars[[i]])
            type.cols[i] <- 1
        } else if (is.character (tmp)) {
            factor.cols <- c(factor.cols, i)
            vars[[i]] <- as.factor(vars[[i]])
            factor.levels <- append(factor.levels, list(levels(vars[[i]])))
            .bm[,i] <- as.numeric(vars[[i]])
            type.cols[i] <- 2
        }
    }
    pad <- rep(0, ncols)
    for (i in seq_len(length(factor.cols))) {
        pad[factor.cols[i]] <- max(nchar(factor.levels[[i]]))
    }

    .master <- structure(list(.bm),
                      factor.cols=factor.cols,
                      factor.levels=factor.levels,
                      type.cols=type.cols,
                      order.cols=order.cols,
                      pad=pad,
                      cl = cl,
                      colnames = names.cols,
                      class = append("fastdf", "list"))
    .desc <- describe (.bm)
    clusterExport (cl, ".desc", envir=environment())
    clusterExport (cl, ".master", envir=environment())
    clusterEvalQ (cl, {.master[[1]] <- attach.big.matrix (.desc); NULL})
    clusterEvalQ (cl, {.local <- .master; NULL})
    return (.master)
}

#' @export
as.fastdf <- function (x, cl=NULL) {
    UseMethod ("as.fastdf")
}

#' @export
as.fastdf.data.frame <- function (x, cl=NULL) {
    return (fastdf (x))
}

.p <- function (...) { paste (..., sep="") }

#' @export
pad.cols <- function (x, max.row=10) {
    #FIXME: more efficient way of doing this?
    if (is.null (max.row) || max.row==0) {
        max.row <- nrow(x[[1]])
    }
    pc <- attr(x, "pad")
    for (i in seq_len(ncol(x[[1]]))[pc==0]) {
        pc[i] <- max(nchar(as.character(x[[1]][1:max.row,i])))
    }
    pc
}

#' @export
print.fastdf <- function (x, max.row = 10) {
    if (is.null(max.row) || max.row == 0) {
        max.row <- nrow(x[[1]])
    }

    ord <- attr(x, "order.cols")
    cols <- seq_len(length(ord))[order(ord)]
    cols <- cols[ord[order(ord)] > 0]

    cat (sprintf ("\n    Fast data frame\n\n"))
    pc <- pad.cols(x, max.row)
    out <- ""
    for (i in cols) {
        out <- .p(out,
                  sprintf(.p("%",pc[i],"s "),
                      attr(x, "colnames")[i]))
    }
    cat (.p(out, "\n"))

    rows <- bigmemory::mwhich (x[[1]],
                    cols=match(".filter", attr(x, "colnames")),
                    vals=1,
                    comps=list("eq"))
    rows.avail <- length(rows)
    if ((is.null(max.row) || max.row == 0) && rows.avail > max.row) {
        rows <- rows[1:max.row]
    }

    for (i in rows) {
        out <- ""
        for (j in cols) {
            v <- x[[1]][i,j]
            if (attr(x, "type.cols")[j] > 0) {
                f <- match (j, attr(x, "factor.cols"))
                v <- attr(x, "factor.levels")[[f]][v]
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
`$.fastdf` <- function (x, name) {
    i <- match (name, attr(x, "colnames"))
    itype <- attr(x, "type.cols")[i]
    if (itype == 0) {
        return (x[[1]][,i])
    } else if (itype == 1) {
        f <- match (i, attr(x, "factor.cols"))
        return (factor(x[[1]][,i],
                             levels=seq_len(length(attr(x, "factor.levels")[[f]])),
                             labels=attr(x, "factor.levels")[[f]]))
    } else if (itype == 2) {
        f <- match (i, attr(x, "factor.cols"))
        return (attr(x, "factor.levels")[[f]][x[[1]][,i]])
    }
}

#' @export
`[.fastdf` <- function (x, cols) {
    m <- match (cols, attr(x, "colnames"))
    out <- NULL
    for (i in m) {
        itype <- attr(x, "type.cols")[i]
        if (itype == 0) {
            o <- (x[[1]][,i])
        } else if (itype == 1) {
            f <- match (i, attr(x, "factor.cols"))
            o <- (factor(x[[1]][,i],
                           levels=seq_len(length(attr(x, "factor.levels")[[f]])),
                           labels=attr(x, "factor.levels")[[f]]))
        } else if (itype == 2) {
            f <- match (i, attr(x, "factor.cols"))
            o <- (attr(x, "factor.levels")[[f]][x[[1]][,i]])
        }
        if (is.null(out)) {
            out <- o
        } else {
            out <- cbind (data.frame(out), data.frame(o))
        }
    }
    if (length(m) > 1) {
        names (out) <- attr(x, "colnames")[m]
    }
    return (out)
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
bind_variables <- function (dat, envir) {
    rm (list=ls(envir=envir), envir=envir)
    for (var in names(dat)) {
        f <- local ({.var<-var; .dat<-dat; function (x) {.dat[.var]}})
        makeActiveBinding(var, f, env=envir)
    }
    return (ls(envir=envir))
}

#' @export
with.fastdf <- function (data, expr, ...) {
    eval (substitute(expr), as.environment.fastdf(data), enclos = parent.frame())
}

#' @export
as.environment.fastdf <- function (x) {
    attr(x, "bindenv") <- new.env()
    bind_variables (x, attr(x, "bindenv"))
    attr (x, "bindenv")
}
