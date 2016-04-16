
#' Constructor
#' @param .cl Cluster object, number of nodes or NULL (default)
#' @param alloc Allocate additional space
#' @export
fastdf <- function (..., alloc=1, cl = NULL) {
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

    parallel::clusterEvalQ (cl, library(multiplyr))
    parallel::clusterEvalQ (cl, library(lazyeval))

    special <- c(".filter", ".group")
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
                      nsa = FALSE,
                      grouped = FALSE,
                      group = 0,
                      class = append("fastdf", "list"))
    .desc <- describe (.bm)
    parallel::clusterExport (cl, ".desc", envir=environment())
    parallel::clusterExport (cl, ".master", envir=environment())
    parallel::clusterEvalQ (cl, {
        .master[[1]] <- attach.big.matrix (.desc)
        attr(.master, "cl") <- NULL
        NULL
    })
    return (.master %>% partition())
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
    if (is.null(max.row) || max.row == 0) {
        max.row <- nrow(x[[1]])
    }
    if (rows.avail > max.row) {
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
    filtercol <- match (".filter", attr(x, "colnames"))
    filtered <- x[[1]][, filtercol] == 1
    if (itype == 0) {
        return (x[[1]][filtered,i])
    } else if (itype == 1) {
        f <- match (i, attr(x, "factor.cols"))
        return (factor(x[[1]][filtered,i],
                             levels=seq_len(length(attr(x, "factor.levels")[[f]])),
                             labels=attr(x, "factor.levels")[[f]]))
    } else if (itype == 2) {
        f <- match (i, attr(x, "factor.cols"))
        return (attr(x, "factor.levels")[[f]][x[[1]][filtered,i]])
    }
}

#' @export
`[.fastdf` <- function (x, cols) {
    m <- match (cols, attr(x, "colnames"))
    filtercol <- match (".filter", attr(x, "colnames"))
    filtered <- x[[1]][, filtercol] == 1
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
        o <- o[filtered]
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
        if (!attr(dat, "nsa")) {
            f <- local ({
                .var<-var
                .dat<-dat
                function (x) {
                    .dat[.var]
                }
            })
        } else {
            f <- local ({
                .var<-var
                .dat<-dat
                function (x) {
                    .col <- match(.var, attr(.dat, "colnames"))
                    .dat[[1]][, .col]
                }
            })
        }
        makeActiveBinding(var, f, env=envir)
    }
    return (ls(envir=envir))
}

#' @export
group_restrict <- function (dat, group) {
    if (group <= 0) { return (dat) }
    attr(dat, "group") <- group

    #presumes that dat is sorted by grouping column first
    Gcol <- match (".group", attr(dat, "colnames"))
    lims <- range(which (dat[[1]][, Gcol] == attr(dat, "group")))
    dat[[1]] <- bigmemory::sub.big.matrix(dat[[1]],
                                          firstRow=lims[1],
                                          lastRow=lims[2])

    return (dat)
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

#' @export
no.strings.attached <- function (x) {
    attr(x, "nsa") <- TRUE
    x
}

#' @export
distribute <- function (x, N) {
    if (length(x) == 1) {
        res <- rep(floor(x / N), N)
        rem <- (x - sum(res))       #left over from rounding
        i <- sample(1:N, rem)       #load balance
        res[i] <- res[i] + 1
        return (res)
    } else {
        #FIXME: naive bin packing algorithm
        bin.indices <- list()
        bin.size <- c()
        for (i in order(x, decreasing=TRUE)) {
            if (length(bin.size) < N) {
                bin.indices <- append (bin.indices, c(i))
                bin.size <- c(bin.size, x[i])
            } else {
                b <- which (bin.size == min(bin.size))
                if (length(b) > 1) {
                    b <- sample (b, 1)
                }
                bin.indices[[b]] <- c(bin.indices[[b]], i)
                bin.size[b] <- bin.size[b] + x[i]
            }
        }
        return (bin.indices)
    }
}
