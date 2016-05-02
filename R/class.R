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
                bm.master       = "big.matrix",
                desc            = "big.matrix.descriptor",
                cls             = "SOCKcluster",
                factor.cols     = "numeric",
                factor.levels   = "list",
                type.cols       = "numeric",
                order.cols      = "numeric",
                pad             = "numeric",
                col.names       = "character",
                nsa             = "logical",
                grouped         = "logical",
                group           = "numeric",
                group_partition = "logical",
                group.cols      = "numeric",
                group_sizes     = "numeric",
                group_max       = "numeric",
                bindenv         = "environment",
                first           = "numeric",
                last            = "numeric",
                filtercol       = "numeric",
                groupcol        = "numeric",
                tmpcol          = "numeric",
                empty           = "logical",
                filtered        = "logical",
                auto_compact    = "logical",
                auto_partition  = "logical",
                group_sizes_stale = "logical"
                ),
    methods=list(
initialize = function (..., alloc=1, cl=NULL,
                       auto_compact=TRUE, auto_partition=TRUE) {
    vars <- list(...)

    if (length(vars) == 0) {
        #Occurs when $copy() used
        #FIXME: how to manage when user calls
        return()
    } else if (length(vars) == 1) {
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

    special <- c(".filter", ".group", ".tmp")
    nrows <- length(vars[[1]])
    ncols <- length(vars) + alloc + length(special)
    col.names <<- c(names(vars), rep(NA, alloc), special)
    order.cols <<- c(seq_len(length(vars)), rep(0, alloc), rep(0, length(special)))
    Rdsm::mgrmakevar(cls, ".bm", nr=nrows, nc=ncols)

    bm <<- .bm
    bm.master <<- .bm

    first <<- 1
    last <<- nrows
    filtercol <<- match (".filter", col.names)
    groupcol <<- match (".group", col.names)
    tmpcol <<- match (".tmp", col.names)

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
    empty <<- nrows > 0
    filtered <<- FALSE
    auto_compact <<- auto_compact
    auto_partition <<- auto_partition
    group_sizes_stale <<- FALSE

    desc <<- bigmemory.sri::describe (.bm)
    .master <- .self
    cluster_export (".master")
    cluster_eval ({
        .master$reattach()
        NULL
    })
    partition_even()
},
copy = function (shallow = FALSE) {
    if (!shallow) {
        stop ("Non-shallow copy not implemented safely yet")
    }
    callSuper (shallow)
},
group_restrict = function (group) {
    if (group <= 0) { return (.self) }
    grp <- .self$copy (shallow=TRUE)
    grp$group_sizes <- grp$group_sizes[grp$group == group]
    grp$group <- group

    #presumes that dat is sorted by grouping column first
    rows <- which (grp$bm[, grp$groupcol] == grp$group)
    if (length(rows) == 0) {
        grp$empty <- TRUE
        return (grp)
    }
    lims <- range(rows)
    grp$bm <- bigmemory::sub.big.matrix(grp$bm,
                                          firstRow=lims[1],
                                          lastRow=lims[2])
    grp$first <- lims[1]
    grp$last <- lims[2]
    grp$empty <- FALSE
    return (grp)
},
reattach = function (descriptor = desc) {
    bm <<- bigmemory::attach.big.matrix(descriptor)
    bm.master <<- bm
},
cluster_export = function (var, var.as=NULL, envir=parent.frame()) {
    if (is.null(var.as)) {
        parallel::clusterExport (cls, var, envir)
    } else {
        if (length(var) != length(var.as)) {
            stop ("var.as needs to be same length as var: did you mean to do cluster_export(c(...))?")
        }
        tmp <- new.env()
        for (i in 1:length(var)) {
            assign (var.as[i], get(var[i], envir=envir), envir=tmp)
            parallel::clusterExport (cls, var.as, envir=tmp)
        }
    }
},
cluster_export_each = function (var, var.as=var, envir=parent.frame()) {
    if (length(var) != length(var.as)) {
        stop ("var.as needs to be same length as var: did you mean to do cluster_export_each(c(...))?")
    }
    tmp <- new.env()
    for (i in 1:length(var)) {
        for (j in 1:length(cls)) {
            assign (var.as[i], get(var[i], envir=envir)[[j]], envir=tmp)
            parallel::clusterExport (cls[j], var.as[i], envir=tmp)
        }
    }
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
    if (grouped) {
        cat (sprintf ("\nGrouped by: %s\n",
                      paste(col.names[group.cols], collapse=", ")))
        .self$calc_group_sizes (delay=FALSE)
        cat (sprintf ("Groups: %d\nGroup sizes: %s\n", group_max,
                      paste(group_sizes, collapse=", ")))
    }
    if (group_partition) {
        res <- cluster_eval ({
            if (.local$empty) { return(0) }
            return (length(.groups))
        })
        res <- do.call (c, res)
        cat (sprintf ("Group partioned over %d clusters\n", length(cls)))
        cat (sprintf ("Groups per cluster: %s\n", paste(res, collapse=", ")))
    } else {
        res <- cluster_eval ({
            if (.local$empty) { return(0) }
            return (nrow(.local$bm))
        })
        cat (sprintf ("\nData partitioned over %d clusters\n", length(cls)))
        if (grouped) {
            cat ("Warning: You may want to run partition_group() as each cluster has partial groups\n")
        } else {
            cat (sprintf ("N per cluster: %s\n", paste(res, collapse=", ")))
        }
    }
    if (filtered) {
        cat ("\nData filtering is in place: you may want to compact (or enable auto_compact)\n")
    }
    cat ("\n")
},
get_data = function (i=NULL, j=NULL, nsa=FALSE) {
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
    if (filtered) {
        filtrows <- bm[, filtercol] == 1
        if (!is.null(rowslice)) {
            filtrows[-rowslice] <- FALSE
        }
        if (nsa) {
            return (bm[filtrows, cols])
        }
    } else if (!is.null(rowslice)) {
        if (nsa) {
            return (bm[rowslice, cols])
        }
        filtrows <- rowslice
    } else {
        filtrows <- NULL
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
        if (!is.null(filtrows)) {
            o <- o[filtrows]
        }
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
set_data = function (i=NULL, j=NULL, value, nsa=FALSE) {
    len <- length(value)

    if (is.null(i)) {
        rowslice <- NULL
    } else {
        rowslice <- i
    }
    if (!is.null(j)) {
        if (!is.numeric(j)) {
            j <- match (j, col.names)
        }
    }

    if (filtered) {
        filtrows <- bm[, filtercol] == 1
        if (!is.null(rowslice)) {
            filtrows[-rowslice] <- FALSE
        }
        nr <- sum(filtrows)
    } else if (!is.null(rowslice)) {
        filtrows <- rowslice
        if (is.logical(rowslice)) {
            nr <- sum(rowslice)
        } else {
            nr <- length(rowslice)
        }
    } else {
        filtrows <- NULL
        nr <- (last - first) + 1
    }

    if (is.null(i)) {
        # [, j] <-
        # [j] <-
        if (len == 1 || len == nr) {
            if (is.null(filtrows)) {
                if (nsa) {
                    bm[, j] <<- value
                } else {
                    bm[, j] <<- factor_map (j, value)
                }
            } else {
                if (nsa) {
                    bm[filtrows, j] <<- value
                } else {
                    bm[filtrows, j] <<- factor_map (j, value)
                }
            }
        } else {
            stop (sprintf("replacement data has %d rows to replace %d", len, nr))
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
                    .dat$get_data(NULL, .var, .nsa)
                } else {
                    .dat$set_data(NULL, .var, x, .nsa)
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
        if (groupcol %in% cols) {
            cols <- cols[cols != groupcol]
        }
        cols <- c(groupcol, cols)
    }
    bigmemory::mpermute (bm, cols=cols)
},
alloc_col = function (name=".tmp", update=FALSE) {
    res <- match (name, col.names)
    if (all(!is.na(res))) {
        return (res)
    }
    needalloc <- which (is.na(res))
    avail <- which (is.na (col.names))
    if (length(needalloc) > length(avail)) {
        stop ("Insufficient free columns available")
    }

    alloced <- avail[1:length(needalloc)]
    res[needalloc] <- alloced

    col.names[alloced] <<- name[needalloc]
    type.cols[alloced] <<- 0
    order.cols[alloced] <<- max(order.cols) + 1:length(alloced)

    if (update) {
        update_fields (c("col.names", "type.cols", "order.cols"))
    }
    return (res)
},
free_col = function (cols, update=FALSE) {
    fc <- type.cols[cols] > 0
    if (any(fc)) {
        fc <- cols[fc]
        if (length(fc) == 1) {
            idx <- -match (fc, factor.cols)
        } else {
            keep <- setdiff (factor.cols, fc)
            idx <- match (keep, factor.cols)
        }
        factor.cols <<- factor.cols[idx]
        factor.levels <<- factor.levels[idx]
        if (update) {
            #FIXME: do factor.levels drop rather than resend
            update_fields (c("factor.cols", "factor.levels"))
        }
    }
    col.names[cols] <<- NA
    type.cols[cols] <<- 0
    order.cols[cols] <<- 0
    if (update) {
        update_fields (c("col.names", "type.cols", "order.cols"))
    }
},
update_fields = function (fieldnames) {
    idx <- match ("grouped", fieldnames)
    do_grouped <- !is.na(idx)
    if (do_grouped) {
        fieldnames <- fieldnames[-idx]
    }

    for (.fieldname in fieldnames) {
        .fieldval <- .self$field(name=.fieldname)
        cluster_export (c(".fieldname", ".fieldval"))
        cluster_eval({
            .master$field (name = .fieldname, value = .fieldval)
            .local$field (name = .fieldname, value = .fieldval)
            if (.local$empty) { return(NULL) }
            if (.local$grouped) {
                for (.g in 1:length(.grouped)) {
                    .grouped[[.g]]$field (name = .fieldname, value = .fieldval)
                }
            }
            NULL
        })
    }

    if (do_grouped) {
        .i <- grouped
        cluster_export (".i")
        cluster_eval ({.master$grouped <- .local$grouped <- .i})
    }
},
partition_even = function (max.row = last) {
    N <- length(cls)

    nr <- distribute (max.row, N)
    if (max.row < N) {
        nr[nr != 0] <- 1:max.row
        cluster_export_each ("nr", ".first")
        cluster_export_each ("nr", ".last")
    } else {
        .last <- cumsum(nr)
        .first <- c(0, .last)[1:N] + 1
        cluster_export_each (".first")
        cluster_export_each (".last")
    }

    cluster_eval ({
        if (!exists(".local")) {
            .local <- .master$copy (shallow=TRUE)
        }
    })

    grouped <<- group_partition <<- FALSE
    update_fields (c("grouped", "group_partition"))

    cluster_eval ({
        .local$empty <- (.last < .first || .last == 0)
        if (.local$empty) { return(NULL) }
        .local$local_subset (.first, .last)
        NULL
    })
},
local_subset = function (first, last) {
    first <<- first
    last <<- last
    bm <<- bigmemory::sub.big.matrix (bm.master, firstRow=first, lastRow=last)
},
rebuild_grouped = function () {
    cluster_eval ({
        if (.local$empty) { return(NULL) }

        .grouped <- list()
        for (.g in 1:length(.groups)) {
            .grp <- .local$group_restrict(.groups[.g])
            .grouped <- append(.grouped, list(.grp))
        }

        NULL
    })
},
filter_rows = function (rows) {
    bm[, tmpcol] <<- 0
    bm[rows, tmpcol] <<- 1

    bm[, filtercol] <<- bm[, filtercol] * bm[, tmpcol]
    empty <<- sum(bm[, filtercol]) == 0
    filtered <<- TRUE
},
filter_vector = function (rows) {
    bm[, filtercol] <<- bm[, filtercol] * rows
    empty <<- sum(bm[, filtercol]) == 0
    filtered <<- TRUE
},
filter_range = function (start, end) {
    if (start > 1) {
        bm[1:(start-1), filtercol] <<- 0
    }
    if (end < nrow(bm)) {
        bm[(end+1):nrow(bm), filtercol] <<- 0
    }
    filtered <<- TRUE
},
compact = function () {
    if (!filtered) { return() }

    rg_grouped <- grouped
    rg_partion <- group_partition
    rg_cols <- group.cols

    partition_even()
    N <- cluster_eval ({
        #(1) Sort by filtercol decreasing
        bigmemory::mpermute (.local$bm, cols=.local$filtercol, decreasing=TRUE)

        #(2) Find the length of each included block
        .N <- sum(.local$bm[, .local$filtercol])
    })
    N <- do.call (c, N)

    #(3) Assign each node a target range
    dest <- cumsum(N)
    last <<- dest[length(dest)]
    dest <- dest[-length(dest)]
    dest <- c(0, dest) + 1
    cluster_export_each ("dest", ".dest")

    #(4) Within each node/group, move data to target range
    cluster_eval ({
        if (.N == 0) { return(NULL) }
        .local$bm.master[.dest:(.dest+.N-1),] <- .local$bm[1:.N,]
        NULL
    })

    #(5) Submatrix master, propagate to local
    filtered <<- FALSE
    .self$bm <- bigmemory::sub.big.matrix (.self$bm, firstRow=1, lastRow=last)
    cluster_export ("last", ".last")
    cluster_eval ({
        .master$last <- .last
        .master$filtered <- FALSE
        .master$bm <- bigmemory::sub.big.matrix (.master$bm, firstRow=1, lastRow=.master$last)
        rm (.local)
        NULL
    })

    #(6) Regroup/partition
    partition_even()

    grouped <<- rg_grouped
    group_partition <<- rg_partion
    group.cols <<- rg_cols

    #(group_by_ will call rebuild_grouped and partition_group)
    if (grouped) {
        group_by_ (.self, .cols=rg_cols)
    }
},
calc_group_sizes = function (delay=TRUE) {
    if (delay) {
        group_sizes_stale <<- TRUE
        return()
    }
    if (!group_sizes_stale) {
        return()
    }
    #FIXME: make parallel/more efficient
    if (filtered) {
        bm[, tmpcol] <<- bm[, groupcol] * bm[, filtercol]
        for (g in 1:group_max) {
            group_sizes[g] <<- sum(bm[, tmpcol] == g)
        }
    } else {
        for (g in 1:group_max) {
            group_sizes[g] <<- sum(bm[, groupcol] == g)
        }
    }
    group_sizes_stale <<- FALSE
}
))

