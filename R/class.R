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
        } else if (is(vars[[1]], "Multiplyr.desc")) {
            reattach (vars[[1]])
            return()
        }
    }

    if (is.null (cl)) {
        cl <- max (1, parallel::detectCores() - 1)
        cls <<- parallel::makeCluster (cl)
    } else if (is.numeric(cl)) {
        cls <<- parallel::makeCluster(cl)
    } else {
        cls <<- cl
        cluster_eval ({
            if (exists(".master")) { rm(.master) }
            if (exists(".local")) { rm(.local) }
            NULL
        })
    }

    res <- do.call (c, cluster_eval(exists("partition_even")))
    if (any(!res)) {
        cluster_eval ({
            library (multiplyr)
            library (lazyeval)
        })
    }

    special <- c(".filter", ".group", ".tmp")
    nrows <- length(vars[[1]])
    ncols <- length(vars) + alloc + length(special)
    col.names <<- c(names(vars), rep(NA, alloc), special)
    order.cols <<- c(seq_len(length(vars)), rep(0, alloc), rep(0, length(special)))

    bm <<- bigmemory::big.matrix (nrow=nrows, ncol=ncols)
    bm.master <<- bm

    first <<- 1
    last <<- nrows
    filtercol <<- match (".filter", col.names)
    groupcol <<- match (".group", col.names)
    tmpcol <<- match (".tmp", col.names)

    bm[,filtercol] <<- 1

    type.cols <<- rep(0, ncols)

    for (i in 1:length(vars)) {
        tmp <- vars[[i]][1]
        if (is.numeric (tmp)) {
            bm[,i] <<- vars[[i]]
        } else if (is.factor (tmp)) {
            factor.cols <<- c(factor.cols, i)
            factor.levels <<- append(factor.levels, list(levels(vars[[i]])))
            type.cols[i] <<- 1
            #These next two lines are faster version of as.numeric
            attr(vars[[i]], "levels") <- NULL
            bm[,i] <<- unclass(vars[[i]])
        } else if (is.character (tmp)) {
            factor.cols <<- c(factor.cols, i)
            vars[[i]] <- as.factor(vars[[i]])
            factor.levels <<- append(factor.levels, list(levels(vars[[i]])))
            type.cols[i] <<- 2
            #These next two lines are faster version of as.numeric
            attr(vars[[i]], "levels") <- NULL
            bm[,i] <<- unclass(vars[[i]])
        }
    }
    pad <<- rep(0, ncols)
    for (i in seq_len(length(factor.cols))) {
        pad[factor.cols[i]] <<- max(nchar(factor.levels[[i]]))
    }

    nsa <<- FALSE
    group.cols <<- 0
    grouped <<- FALSE
    group <<- 0
    group_partition <<- FALSE
    empty <<- nrows == 0
    filtered <<- FALSE
    auto_compact <<- auto_compact
    auto_partition <<- auto_partition
    group_sizes_stale <<- FALSE

    desc <<- bigmemory.sri::describe (bm)

    cluster_export_self ()

    partition_even()
},
cluster_export_self = function () {
    #Replaces cluster_export (".master")
    .res <- .self$describe()
    cluster_export (".res")
    cluster_eval({
        .master <- Multiplyr(.res)
        NULL
    })
},
describe = function () {
    fnames <- names(.refClassDef@fieldClasses)
    fnames <- as.list(fnames[-match(c("bm", "bm.master", "cls"), fnames)])
    out <- lapply(fnames, function (x, d) { d$field(x) }, .self)
    names(out) <- fnames
    class(out) <- "Multiplyr.desc"
    return (out)
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
    grp$bm <- bigmemory::sub.big.matrix(grp$desc,
                                          firstRow=lims[1],
                                          lastRow=lims[2])
    grp$desc <- sm_desc_update (grp$desc, lims[1], lims[2])
    grp$first <- lims[1]
    grp$last <- lims[2]
    grp$empty <- FALSE
    return (grp)
},
reattach = function (descres) {
    nm <- names(descres)
    for (i in 1:length(descres)) {
        field(nm[i], descres[[i]])
    }

    bm <<- bigmemory::attach.big.matrix(desc)
    bm.master <<- bm

    #horrible kludge so copy() doesn't complain about NULL
    #as apparently R can't cope with assigning NULL to fields?
    clsna <- NA
    class(clsna) <- "SOCKcluster"
    cls <<- clsna
},
cluster_export = function (var, var.as=NULL, envir=parent.frame()) {
    if (is.null(var.as)) {
        parallel::clusterExport (cls, var, envir) #PROFME
    } else {
        if (length(var) != length(var.as)) {
            stop ("var.as needs to be same length as var: did you mean to do cluster_export(c(...))?")
        }
        tmp <- new.env()
        for (i in 1:length(var)) {
            assign (var.as[i], get(var[i], envir=envir), envir=tmp)
            parallel::clusterExport (cls, var.as[i], envir=tmp)
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
get_data = function (i=NULL, j=NULL, nsa=FALSE, drop=TRUE) {
    if (is.null(i)) {
        rowslice <- NULL
    } else {
        rowslice <- i
    }

    if (is.null(j)) {
        cols <- which (order.cols > 0)
        if (length(cols) == 0) {
            return (data.frame())
        }
    } else if (is.numeric(j)) {
        if (any(j < 1)) {
            stop (sprintf ("Invalid reference to column: %d < 1", (j[j < 1])[1]))
        } else if (any(j > ncol(bm))) {
            stop (sprintf("Invalid reference to column: %d > %d", (j[j > ncol(bm)])[1], ncol(bm)))
        }
        cols <- j
    } else {
        cols <- match (j, col.names)
        cols.na <- is.na(cols)
        if (any(cols.na)) {
            stop (.p("Undefined column(s): ", paste0(j[cols.na], collapse=", ")))
        }
    }

    if (empty) {
        l <- lapply(type.cols[cols], function (x) { if (x==0) { numeric(0) } else { character(0) } })
        if (length(l) == 1) {
            return (l[[1]])
        } else {
            class(l) <- "data.frame"
            colnames(l) <- col.names[cols]
            return (l)
        }
    }

    if (is.null(rowslice)) {
        if (filtered) {
            filtrows <- bm[, filtercol] == 1
        } else {
            filtrows <- NULL
        }
    } else {
        if (is.numeric(rowslice)) {
            if (min(rowslice) < 1) {
                stop (sprintf("Invalid row reference: %d < 1", min(rowslice)))
            }
            if (filtered) {
                filtrows <- which(bm[, filtercol] == 1)
                if (max(rowslice) > length(filtrows)) {
                    stop (sprintf("Invalid row reference: %d > %d", max(rowslice), length(filtrows)))
                }
                filtrows <- filtrows[-rowslice]
            } else {
                if (max(rowslice) > ((last-first+1))) {
                    stop (sprintf("Invalid row reference: %d > %d", max(rowslice), (last-first)+1))
                }
                filtrows <- rowslice
            }
        } else if (is.logical(rowslice)) {
            if (filtered) {
                filtrows <- bm[, filtercol] == 1
                if (sum(filtrows) %% length(rowslice) != 0) {
                    stop ("Number of available rows needs to be an exact multiple of rowslice length")
                }
                filtrows <- filtrows[!rowslice]
            } else {
                if (((last-first)+1) %% length(rowslice) != 0) {
                    stop ("Number of available rows needs to be an exact multiple of rowslice length")
                }
                filtrows <- rowslice
            }
        }
    }

    if (nsa) {
        if (!is.null(filtrows)) {
            return (bm[filtrows, cols])
        } else {
            return (bm[, cols])
        }
    }

    out <- NULL
    for (i in cols) {
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

    if (!drop) {
        out <- data.frame(out)
    }

    if (length(cols) > 1 || !drop) {
        colnames (out) <- col.names[cols]
    }

    return (out)
},
set_data = function (i=NULL, j=NULL, value, nsa=FALSE) {
    if (is.null(i)) {
        rowslice <- NULL
    } else {
        rowslice <- i
    }

    if (!is.null(j)) {
        if (is.numeric(j)) {
            if (any(j < 1)) {
                stop (sprintf ("Invalid reference to column: %d < 1", (j[j < 1])[1]))
            } else if (any(j > ncol(bm))) {
                stop (sprintf("Invalid reference to column: %d > %d", (j[j > ncol(bm)])[1], ncol(bm)))
            }
            cols <- j
        } else {
            cols <- match (j, col.names)
            cols.na <- is.na(cols)
            if (any(cols.na)) {
                stop (.p("Undefined column(s): ", paste0(j[cols.na], collapse=", ")))
            }
        }
    }

    if (is.null(rowslice)) {
        if (filtered) {
            filtrows <- bm[, filtercol] == 1
            nr <- sum(filtrows)
        } else {
            nr <- (last - first) + 1
            filtrows <- NULL
        }
    } else {
        if (is.numeric(rowslice)) {
            if (min(rowslice) < 1) {
                stop (sprintf("Invalid row reference: %d < 1", min(rowslice)))
            }
            if (filtered) {
                filtrows <- which(bm[, filtercol] == 1)
                nr <- length (filtrows)
                if (max(rowslice) > nr) {
                    stop (sprintf("Invalid row reference: %d > %d", max(rowslice), nr))
                }
                filtrows <- filtrows[-rowslice]
            } else {
                nr <- (last - first) + 1
                if (max(rowslice) > nr) {
                    stop (sprintf("Invalid row reference: %d > %d", max(rowslice), nr))
                }
                filtrows <- rowslice
            }
        } else if (is.logical(rowslice)) {
            if (filtered) {
                filtrows <- bm[, filtercol] == 1
                nr <- sum(filtrows)
                if (nr %% length(rowslice) != 0) {
                    stop ("Number of available rows needs to be an exact multiple of rowslice length")
                }
                filtrows <- filtrows[!rowslice]
            } else {
                nr <- (last - first) + 1
                if (nr %% length(rowslice) != 0) {
                    stop ("Number of available rows needs to be an exact multiple of rowslice length")
                }
                filtrows <- rowslice
            }
        }
    }

    if (is.null(i)) {
        # [, j] <-
        # [j] <-
        len <- length(value)
        if (len == 1 || len == nr) {
            if (is.null(filtrows)) {
                if (nsa) {
                    bm[, cols] <<- value
                } else {
                    bm[, cols] <<- factor_map (cols, value)
                }
            } else {
                if (nsa) {
                    bm[filtrows, cols] <<- value
                } else {
                    bm[filtrows, cols] <<- factor_map (cols, value)
                }
            }
        } else {
            stop (sprintf("replacement data has %d rows to replace %d", len, nr))
        }
    } else {
        if (is.null(j)) {
            # [i, ] <-
            cols <- which (order.cols > 0)
        }
        # [i, j] <-
        if (nsa) {
            if (is.null(filtrows)) {
                bm[, cols] <<- value
            } else {
                bm[filtrows, cols] <<- value
            }
        } else {
            if (is.null(filtrows)) {
                if (length(cols) == 1) {
                    bm[, cols] <<- factor_map(cols, value)
                } else {
                    for (c in 1:length(cols)) {
                        bm[, cols[c]] <<- factor_map(cols[c], value[, c])
                    }
                }
            } else {
                if (length(cols) == 1) {
                    bm[filtrows, cols] <<- factor_map(cols, value)
                } else {
                    for (c in cols) {
                        bm[filtrows, cols[c]] <<- factor_map(cols[c], value[, c])
                    }
                }
            }
        }
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
                    .dat$get_data(NULL, .var, nsa=.nsa, drop=TRUE)
                } else {
                    .dat$set_data(NULL, .var, x, nsa=.nsa)
                }
            }
        })
        makeActiveBinding(var, f, env=bindenv)
    }
    return (bindenv)
},
sort = function (decreasing=FALSE, dots=NULL, cols=NULL, with.group=TRUE) {
    if (empty) { return() }
    if (is.null(cols)) {
        namelist <- .dots2names (dots)
        cols <- match(namelist, col.names)
    }
    if (grouped && with.group) {
        if (groupcol %in% cols) {
            cols <- cols[cols != groupcol]
        }
        cols <- c(groupcol, cols)
    }
    if (length(cols) == 0) {
        stop ("No sorting column(s) specified")
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
    if (empty || max.row == 0) { return() }
    N <- length(cls)

    if (max.row == 0) {
        cluster_eval ({
            if (exists(".master")) {
                .master$empty <- TRUE
            }
            if (exists(".local")) {
                .local$empty <- TRUE
            }
            NULL
        })
        return()
    }

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

    grouped <<- group_partition <<- FALSE

    cluster_eval ({
        if (!exists(".local")) {
            .local <- .master$copy (shallow=TRUE)
        }
        .local$desc <- .master$desc

        .master$grouped <- .local$grouped <- FALSE
        .master$group_partition <- .local$group_partition <- FALSE

        .local$empty <- (.last < .first || .last == 0)
        if (.local$empty) { return(NULL) }
        .local$local_subset (.first, .last)
        NULL
    })

    return()
},
local_subset = function (first, last) {
    if (empty) { return() }
    first <<- first
    last <<- last
    bm <<- bigmemory::sub.big.matrix (desc, firstRow=first, lastRow=last)
    desc <<- sm_desc_update (desc, first, last)
},
rebuild_grouped = function () {
    if (empty) { return() }
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
    if (empty) { return() }
    bm[, tmpcol] <<- 0
    bm[rows, tmpcol] <<- 1

    bm[, filtercol] <<- bm[, filtercol] * bm[, tmpcol]
    empty <<- sum(bm[, filtercol]) == 0
    filtered <<- TRUE
},
filter_vector = function (rows) {
    if (empty) { return() }
    bm[, filtercol] <<- bm[, filtercol] * rows
    empty <<- sum(bm[, filtercol]) == 0
    filtered <<- TRUE
},
filter_range = function (start, end) {
    if (empty) { return() }
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
    if (empty) {
        filtered <<- FALSE
        update_fields ("filtered")
        return ()
    }

    rg_grouped <- grouped
    rg_partion <- group_partition
    rg_cols <- group.cols

    partition_even() #PROFME
    N <- cluster_eval ({ #PROFME
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
    if (last > 0) {
        bm <<- bigmemory::sub.big.matrix (desc, firstRow=1, lastRow=last)
        desc <<- sm_desc_update (desc, 1, last)
        cluster_export ("last", ".last")
        cluster_eval ({
            .master$last <- .last
            .master$filtered <- FALSE
            .master$bm <- bigmemory::sub.big.matrix (.master$desc, firstRow=1, lastRow=.master$last)
            .master$desc <- sm_desc_update (.master$desc, 1, .master$last)
            rm (.local)
            NULL
        })
    } else {
        empty <<- TRUE
        cluster_eval ({
            .master$empty <- TRUE
            NULL
        })
    }

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
    if (empty) {
        group_sizes <<- rep(0, group_max)
    } else if (filtered) {
        bm[, tmpcol] <<- bm[, groupcol] * bm[, filtercol]
        group_sizes <<- sapply(seq_len(group_max), function (g) {
            sum(.self$bm[, .self$tmpcol] == g)
        })
    } else {
        group_sizes <<- sapply(seq_len(group_max), function (g) {
            sum(.self$bm[, .self$groupcol] == g)
        })
    }
    group_sizes_stale <<- FALSE
},
row_names = function () {
    if (empty) {
        return(character(0))
    }
    if (filtered) {
        return (sum(bm[, filtercol] == 1))
    } else {
        return (seq_len((last - first)+1))
    }
}
))

setMethod ("[", "Multiplyr", function (x, i, j, ..., drop=TRUE) {
    N <- nargs() - !missing(drop) - !missing(...)
    if (N < 3) {
        if (missing(i)) {
            # dat[]
            return (x$get_data(NULL, NULL, drop=drop, ...))
        } else {
            # dat["x"]
            return (x$get_data(NULL, i, drop=drop, ...))
        }
    } else {
        if (missing(i)) {
            # dat[, "x"]
            return (x$get_data(NULL, j, drop=drop, ...))
        } else if (missing(j)) {
            # dat[1:10, ]
            return (x$get_data(i, NULL, drop=drop, ...))
        } else {
            # dat[1:10, "x"]
            return (x$get_data(i, j, drop=drop))
        }
    }
})

setMethod ("[<-", "Multiplyr", function (x, i, j, ..., value) {
    N <- nargs() - !missing(...)
    if (N < 4) {
        if (missing(i)) {
            #dat[] <- val
            x$set_data (NULL, NULL, value, ...)
        } else {
            #dat["x"] <- val
            x$set_data (NULL, i, value, ...)
        }
    } else {
        if (missing(i)) {
            #dat[,"x"] <- val
            x$set_data (NULL, j, value, ...)
        } else if (missing(j)) {
            #dat[1:10, ] <- val
            x$set_data(i, NULL, value, ...)
        } else {
            #dat[1:10, "x"] <- val
            x$set_data(i, j, value, ...)
        }
    }
    invisible (x)
})

setMethod ("as.data.frame", "Multiplyr", function (x) {
    x[]
})

setMethod ("dimnames", "Multiplyr", function (x) {
    list(row.names(x), names(x))
})

setMethod("names", "Multiplyr", function(x) {
    x$col.names[x$order.cols > 0]
})

setMethod("row.names", "Multiplyr", function (x) {
    x$row_names()
})

