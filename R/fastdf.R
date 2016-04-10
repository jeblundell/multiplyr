
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

    nrows <- length(vars[[1]])
    ncols <- length(vars) + alloc
    Rdsm::mgrmakevar(cl, ".bm", nr=nrows, nc=ncols)

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
    order.cols <- c(seq_len(length(vars)), rep(0, alloc))

    return (structure(list(.bm),
                      factor.cols=factor.cols,
                      factor.levels=factor.levels,
                      type.cols=type.cols,
                      order.cols=order.cols,
                      pad=pad,
                      cl = cl,
                      colnames = names(vars),
                      class = append("fastdf", "list")))
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

    cat (sprintf ("\n    Fast data frame\n\n"))
    pc <- pad.cols(x, max.row)
    out <- ""
    for (i in seq_len(ncol(x[[1]]))) {
        out <- .p(out,
                  sprintf(.p("%",pc[i],"s "),
                      attr(x, "colnames")[i]))
    }
    cat (.p(out, "\n"))

    for (i in seq_len(max.row)) {
        out <- ""
        for (j in seq_len(ncol(x[[1]]))) {
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
    if (max.row < nrow(x[[1]])) {
        cat (sprintf ("\n... %d of %d rows omitted ...\n",
                      nrow(x[[1]]) - max.row,
                      nrow(x[[1]])))
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
