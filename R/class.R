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
                group_partition = "logical",
                bindenv         = "environment"
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
get = function (i=NULL, j=NULL, nsa=FALSE) {
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
    if (nsa) {
        return (bm[filtered, cols])
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
},
set = function (i=NULL, j=NULL, value, nsa=FALSE) {
    len <- length(value)

    if (!is.null(j)) {
        if (!is.numeric(j)) {
            j <- match (j, col.names)
        }
    }

    if (is.null(i)) {
        # [, j] <-
        # [j] <-
        if (len == 1 || len == nrow(bm)) {
            if (nsa) {
                bm[, j] <<- value
            } else {
                bm[, j] <<- factor_map (j, value)
            }
        } else {
            stop (sprintf("replacement data has %d rows to replace %d", len, nrow(bm)))
        }
    } else if (is.null(j)) {
        # [i, ] <-
        stop ("FIXME: row replace")
    } else {
        # [i, j] <-
        stop ("FIXME: row/col replace")
    }
    invisible (value)
},
factor_map = function (var, vals) {
    if (is.numeric(var)) {
        col <- var
    } else {
        col <- match (var, col.names)
    }

    if (type.cols[col] == 0) {
        return (vals)
    }

    f <- match (col, factor.cols)
    return (match (vals, factor.levels[[f]]))
},
envir = function (nsa=FALSE) {
    if (is.null(bindenv)) {
        bindenv <<- new.env()
    }

    #Remove existing active bindings
    vars.active <- names (which (vapply (ls(envir=bindenv),
                                         bindingIsActive,
                                         FALSE, bindenv)))
    if (length(vars.active) > 0) {
        rm (list=vars.active, envir=bindenv)
    }

    #Allow . to refer to data frame
    makeActiveBinding (".", local({
        .dat <- .self
        function (x) {
            .dat
        }
    }), env=bindenv)

    #Set bindings for remainder of vars
    for (var in col.names) {
        f <- local ({
            .var<-var
            .dat<-.self
            .nsa<-nsa
            function (x) {
                if (missing(x)) {
                    .dat$get(NULL, .var, .nsa)
                } else {
                    .dat$set(NULL, .var, x, .nsa)
                }
            }
        })
        makeActiveBinding(var, f, env=bindenv)
    }
    return (bindenv)
},
sort = function (decreasing=FALSE, dots=NULL, cols=NULL, with.group=FALSE) {
    if (is.null(cols)) {
        namelist <- .dots2names (dots)
        cols <- match(namelist, col.names)
    }
    if (grouped) {
        Gcol <- match(".group", col.names)
        if (Gcol %in% cols) {
            cols <- cols[cols != Gcol]
        }
        cols <- c(Gcol, cols)
    }
    bigmemory::mpermute (bm, cols=cols)
},
alloc_col = function (name=".tmp") {
    res <- which (is.na (col.names))
    if (length(res) == 0) {
        stop ("No free columns available")
    } else {
        col.names[res[1]] <<- name
        type.cols[res[1]] <<- 0
        order.cols[res[1]] <<- max(order.cols)+1
    }
    return (res[1])
},
free_col = function (col) {
    col.names[col] <<- NA
    type.cols[col] <<- 0
    order.cols[col] <<- 0
}
))

