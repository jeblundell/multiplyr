# Multiplyr class functions

setOldClass (c("cluster", "SOCKcluster"))
#' Parallel processing data frame
#'
#' With the exception of calling Multiplyr to create a new data frame, none
#' of the methods/fields here are really intended for general use: it's
#' generally best to stick to the manipulation functions. Run the following command
#' to get a better overview: \code{vignette("basics")}
#'
#' @docType class
#' @param ... Either a data frame or a list of name=value pairs
#' @param cl Cluster object, number of nodes or NULL (default)
#' @param alloc Allocate additional columns
#' @param auto_compact Automatically compact data after filter operations
#' @param auto_partition Automatically re-partition after group_by
#' @param profiling Enable internal profiling code
#' @return Object of class Multiplyr
#' @import methods
#' @exportClass Multiplyr
#' @export Multiplyr
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=2)
#' dat %>% shutdown()
#' dat.df <- data.frame (x=1:100, G=rep(c("A", "B"), each=50))
#' dat <- Multiplyr (dat.df, cl=2)
#' dat %>% shutdown()
#' }
#' @field auto_compact      Compact data after each filtering etc. operation
#' @field auto_partition    Re-partition after group_by
#' @field bindenv           Environment for \code{within_group} etc. operations
#' @field bm                big.matrix (internal representation of data)
#' @field bm.master         big.matrix for certain operations that need non-subsetted data
#' @field cls               SOCKcluster created by parallel package
#' @field col.names         Name of each column; names starting "." are special and NA is a free column
#' @field desc.master       big.matrix.descriptor for setting up shared memory access
#' @field empty             Flag indicating that this data frame is empty
#' @field factor.cols       Which columns are factors/character
#' @field factor.levels     List (same length as factor.cols) containing corresponding factor levels
#' @field filtercol         Which column in bm indicates filtering (1=included, 0=excluded)
#' @field filtered          Flag indicating that this data frame has had filtering applied
#' @field first             Subsetting: first row
#' @field group.cols        Which columns are involved in grouping
#' @field groupcol          Which column in bm contains the group ID
#' @field grouped           Flag indicating whether grouped
#' @field groupenv          List of environments corresponding to group IDs in group
#' @field group_max         Number of groups
#' @field group_partition   Flag indicating that \code{partition_group()} has been used
#' @field group_sizes_stale Flag indicating that group sizes need to be re-calculated
#' @field group             Which group IDs are assigned to this data frame
#' @field last              Subsetting: last row
#' @field nsamode           Flag indicating whether data frame is in no-strings-attached mode
#' @field order.cols        Display order of columns
#' @field pad               Number of spaces to pad each column or 0 for dynamic
#' @field profile_names     Profile names
#' @field profile_real      Total elapsed time for each profile
#' @field profile_rreal     Reference time for total elapsed
#' @field profile_rsys      Reference time for system
#' @field profile_ruser     Reference time for user
#' @field profile_sys       Total system time for each profile
#' @field profile_user      Total user time for each profile
#' @field profiling         Flag indicating that profiling is to be used
#' @field slave             Flag indicating whether cluster_* operations are valid
#' @field tmpcol            Which column may be used for temporary calculations
#' @field type.cols         Column type (0=numeric, 1=character, 2=factor)
Multiplyr <- setRefClass("Multiplyr",
    fields=list(
        auto_compact      = "logical",
        auto_partition    = "logical",
        bindenv           = "environment",
        bm                = "big.matrix",
        bm.master         = "big.matrix",
        cls.created       = "logical",
        cls               = "SOCKcluster",
        col.names         = "character",
        desc.master       = "big.matrix.descriptor",
        empty             = "logical",
        factor.cols       = "numeric",
        factor.levels     = "list",
        filtercol         = "numeric",
        filtered          = "logical",
        first             = "numeric",
        group_cache       = "big.matrix",
        groupcol          = "numeric",
        group.cols        = "numeric",
        grouped           = "logical",
        groupenv          = "list",
        group_max         = "numeric",
        group             = "numeric",
        group_partition   = "logical",
        group_sizes_stale = "logical",
        last              = "numeric",
        nsamode           = "logical",
        nullframe         = "logical",
        order.cols        = "numeric",
        pad               = "numeric",
        profile_names     = "character",
        profile_real      = "numeric",
        profile_rreal     = "numeric",
        profile_rsys      = "numeric",
        profile_ruser     = "numeric",
        profile_sys       = "numeric",
        profile_user      = "numeric",
        profiling         = "logical",
        savestate         = "list",
        slave             = "logical",
        tmpcol            = "numeric",
        type.cols         = "numeric"
    ),
    methods=list(
initialize = function (..., alloc=0, cl=NULL,
                       auto_compact=TRUE,
                       auto_partition=TRUE,
                       profiling=TRUE) {
    "Constructor"

    dots <- dotscapture (...)
    vars <- list(...)

    #Default/NULL values
    auto_compact      <<- auto_compact
    auto_partition    <<- auto_partition
    bindenv           <<- new.env()
    bm                <<- NA_class_("big.matrix")
    bm.master         <<- NA_class_("big.matrix")
    cls.created       <<- FALSE
    cls               <<- NA_class_ ("SOCKcluster")
    col.names         <<- character(0)
    desc.master       <<- NA_class_("big.matrix.descriptor")
    empty             <<- TRUE
    factor.cols       <<- numeric(0)
    factor.levels     <<- list()
    filtercol         <<- 0
    filtered          <<- FALSE
    first             <<- 0
    group             <<- numeric(0)
    group.cols        <<- numeric(0)
    groupcol          <<- 0
    grouped           <<- FALSE
    groupenv          <<- list()
    group_cache       <<- NA_class_("big.matrix")
    group_max         <<- 0
    group_partition   <<- FALSE
    group_sizes_stale <<- FALSE
    last              <<- 0
    nsamode           <<- FALSE
    nullframe         <<- FALSE
    order.cols        <<- numeric(0)
    pad               <<- numeric(0)
    profile_names     <<- character(0)
    profile_ruser     <<- profile_rsys <<- profile_rreal <<- numeric(0)
    profile_user      <<- profile_sys <<- profile_real <<- numeric(0)
    profiling         <<- profiling
    savestate         <<- list()
    slave             <<- TRUE
    tmpcol            <<- 0
    type.cols         <<- numeric(0)

    if (length(vars) == 0) {
        return()
    }

    vnames <- names(dots)

    profile ("start", "initialize")

    if (length(vars) == 1) {
        if (is.data.frame(vars[[1]])) {
            vnames <- names(vars[[1]])
            vars <- unclass(vars[[1]])
        } else if (is(vars[[1]], "Multiplyr.desc")) {
            reattach_slave (vars[[1]])
            profile ("stop", "initialize")
            return()
        }
    }

    profile ("start", "initialize.cluster")
    cluster_start (cl)

    slave <<- FALSE

    profile ("stop", "initialize.cluster")

    profile ("start", "initialize.data")
    special <- c(".filter", ".group", ".tmp")
    nrows <- length(vars[[1]])
    ncols <- length(vars) + alloc + length(special)
    col.names <<- c(vnames, rep(NA, alloc), special)
    order.cols <<- c(seq_len(length(vars)), rep(0, alloc), rep(0, length(special)))

    bm.master <<- bm <<- bigmemory::big.matrix (nrow=nrows, ncol=ncols)
    group_cache <<- NA_class_("big.matrix")

    first <<- 1
    last <<- nrows
    filtercol <<- match (".filter", col.names)
    groupcol <<- match (".group", col.names)
    tmpcol <<- match (".tmp", col.names)

    bm[,filtercol] <<- 1

    type.cols <<- rep(0, ncols)

    profile ("start", "initialize.load")
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
    profile ("stop", "initialize.load")
    profile ("stop", "initialize.data")

    empty <<- nrows == 0

    desc.master <<- bigmemory.sri::describe (bm)

    cluster_export_self ()

    partition_even()
    profile ("stop", "initialize")
},
finalize = function () {
    "Destructor"
    cluster_stop (only.if.started=TRUE)
},
alloc_col = function (name=".tmp", update=FALSE) {
    "Allocate a new column and optionally update cluster nodes to do the same. Returns the column number"
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
build_grouped = function () {
    "Build group environments"
    if (empty) { return() }
    cluster_eval ({
        if (length(.local$group) > 0) {
            .local$groupenv <- replicate (length(.local$group), new.env())
        }
        NULL
    })
    #Allow group_restrict on master node
    groupenv <<- replicate (length(group), new.env())
},
calc_group_sizes = function (delay=TRUE) {
    "Calculate group sizes (if delay=TRUE then this will just mark group sizes as being stale)"
    if (!grouped) {
        return ()
    }
    if (delay) {
        group_sizes_stale <<- TRUE
        return()
    }
    if (!group_sizes_stale) {
        return()
    }

    if (empty) {
        group_cache[, 3] <<- rep(0, group_max)
    } else if (!filtered) {
        group_cache[, 3] <<- (group_cache[, 2] - group_cache[, 1]) + 1
        if (any(group_cache[, 1] == 0)) {
            group_cache[group_cache[, 1] == 0, 3] <<- 0
        }
    } else {
        N <- length(cls)
        if (group_max == 1) {
            Gi <- distribute (1, N)
            Gi[Gi == 0] <- NA
        } else {
            Gi <- distribute (group_cache[, 3], N)
        }
        cluster_export_each ("Gi", ".groups")

        cluster_eval ({
            if (!(NA %in% .groups)) {
                for (.g in .groups) {
                    if (.local$group_cache[.g, 1] > 0 && .local$group_cache[.g, 2] >= .local$group_cache[.g, 1]) {
                        .sm <- .local$submatrix (.local$group_cache[.g, 1], .local$group_cache[.g, 2])
                        .local$group_cache [.g, 3] <- sum(.sm[, .local$filtercol])
                    } else {
                        .local$group_cache [.g, 3] <- 0
                    }
                }
            }
            NULL
        })
    }
    group_sizes_stale <<- FALSE
},
cluster_eval = function (...) {
    "Executes specified expression on cluster"
    if (!cluster_running()) {
        stop ("Cluster not running")
    }
    if (profiling) {
        profile ("start", "cluster_eval")
        res <- parallel::clusterEvalQ (cls, ...)
        profile ("stop", "cluster_eval")
        return (res)
    }
    parallel::clusterEvalQ (cls, ...)
},
cluster_export = function (var, var.as=NULL, envir=parent.frame()) {
    "Exports a variable from current environment to the cluster, optionally with a different name"
    if (!cluster_running()) {
        stop ("Cluster not running")
    }
    profile ("start", "cluster_export")
    if (is.null(var.as)) {
        parallel::clusterExport (cls, var, envir)
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
    profile ("stop", "cluster_export")
},
cluster_export_each = function (var, var.as=var, envir=parent.frame()) {
    "Like cluster_export, but exports only one element of each variable to each node"
    if (!cluster_running()) {
        stop ("Cluster not running")
    }
    profile ("start", "cluster_export_each")
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
    profile ("stop", "cluster_export_each")
},
cluster_export_self = function () {
    "Exports this data frame to the cluster (naming it .local)"
    .res <- .self$describe()
    cluster_export (".res")
    cluster_eval({
        .local <- Multiplyr(.res)
        NULL
    })
},
cluster_profile = function () {
    "Update profile totals to include all nodes' totals (also resets nodes' totals to 0)"
    if (!profiling || slave) { return() }
    res <- cluster_eval({
        res <- .local$profile()
        if (nrow(res) > 0) {
            .local$profile_names <- character(0)
            .local$profile_user <- numeric(0)
            .local$profile_sys <- numeric(0)
        }
        res
    })
    for (i in res) {
        profile_import (i)
    }
},
cluster_running = function () {
    "Checks whether cluster is running"
    if (length(cls) > 1) {
        return (TRUE)
    } else {
        return (!is.na(cls))
    }
},
cluster_start = function (cl=NULL) {
    "Starts a cluster with cl cores if cl is numeric, detectCores()-1 if cl is NULL, or uses specified existing cluster"
    cls.created <<- is.null(cl) || is.numeric(cl)
    if (is.null (cl)) {
        cl <- max (1, getOption("multiplyr.cores"))
        cls <<- parallel::makeCluster (cl)
    } else if (is.numeric(cl)) {
        cls <<- parallel::makeCluster(cl)
    } else {
        cls <<- cl
        cluster_eval ({
            if (exists(".local")) { rm(.local) }
            NULL
        })
    }
    res <- do.call (c, cluster_eval(exists("partition_even")))
    if (any(!res)) {
        cluster_eval ({
            library (multiplyr)
            NULL
        })
    }
},
cluster_stop = function (only.if.started=FALSE) {
    "Stops cluster"
    if (!cluster_running()) { return() }
    if (cls.created || !only.if.started) {
        cluster_eval ({
            if (exists (".local")) {
                rm (.local)
            }
            NULL
        })
        parallel::stopCluster(cls)
    }
    cls <<- NA_class_("SOCKcluster")
},
compact = function () {
    "Re-sorts data so all rows included after filtering are contiguous (and calls sub.big.matrix in the process)"
    if (!filtered) { return() }
    if (empty) {
        filtered <<- FALSE
        update_fields ("filtered")
        return ()
    }

    rg_grouped <- grouped
    rg_partion <- group_partition
    rg_cols <- group.cols

    partition_even()
    N <- cluster_eval ({
        #(1) Sort by filtercol decreasing
        bm_mpermute (.local$bm, cols=.local$filtercol, decreasing=TRUE)

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
        if (.N > 0) {
            .local$bm.master[.dest:(.dest+.N-1),] <- .local$bm[1:.N,]
        }
        NULL
    })

    #(5) Submatrix master, propagate to local
    filtered <<- FALSE
    if (last > 0) {
        bm <<- bigmemory::sub.big.matrix (desc.master, firstRow=1, lastRow=last)
        desc.master <<- sm_desc_update (desc.master, 1, last)
        cluster_export ("last", ".last")
        cluster_eval ({
            .local$filtered <- FALSE
            .local$desc.master <- sm_desc_update (.local$desc.master, 1, .last)
            .local$bm.master <- bigmemory.sri::attach.resource (.local$desc.master)
            NULL
        })
    } else {
        empty <<- TRUE
        cluster_eval ({
            .local$empty <- TRUE
            NULL
        })
    }

    #(6) Regroup/partition
    partition_even()

    grouped <<- rg_grouped
    group_partition <<- rg_partion
    group.cols <<- rg_cols

    if (grouped) {
        .self$sort (decreasing=FALSE, cols=rg_cols, with.group=FALSE)
        nonempty <- (1:group_max) %in% .self$bm[, .self$groupcol]
        group_cache[!nonempty, 1:3] <<- rep(0, 3)
        nonempty <- which (nonempty)

        for (g in nonempty) {
            w <- which(bm[, groupcol] == g)
            group_cache[g, 1] <<- min(w)
            group_cache[g, 2] <<- max(w)
            group_cache[g, 3] <<- length(w)
        }

        partition_group_ (.self)
    }
},
copy = function (shallow = FALSE) {
    "Create a copy of the data frame"

    if (!shallow) {
        stop ("Non-shallow copy not implemented safely yet")
    }

    if (profiling) {
        t1 <- proc.time()
        res <- callSuper (TRUE)
        t2 <- proc.time() - t1
        res$profile_names <- "copy"
        res$profile_sys <- t2[1][[1]]
        res$profile_user <- t2[2][[1]]
        res$profile_real <- t2[3][[1]]
        res$profile_rsys <- res$profile_ruser <- res$profile_rreal <- 0
    } else {
        res <- callSuper (TRUE)
    }

    return (res)
},
describe = function () {
    "Describes data frame (for later use by reattach_slave)"
    fnames <- names(.refClassDef@fieldClasses)
    fnames <- as.list(fnames[-match(c("bm", "bm.master", "group_cache", "cls", "bindenv", "groupenv", "savestate"), fnames)])
    out <- lapply(fnames, function (x, d) { d$field(x) }, .self)
    names(out) <- fnames
    class(out) <- "Multiplyr.desc"
    return (out)
},
destroy_grouped = function () {
    "Removes grouped data on remote nodes"
    if (grouped) {
        cluster_eval ({
            .local$groupenv <- list()
            NULL
        })
    }
},
envir = function (nsa=NULL) {
    "Returns an environment with active bindings to columns (may also temporarily set no strings attached mode)"
    if (is.null(bindenv)) {
        bindenv <<- new.env()
    }

    if (is.null(nsa)) {
        nsa <- nsamode
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
factor_map = function (var, vals) {
    "For a given set of values (numeric or character), map it to be numeric: this is used to store data in big.matrix"
    if (is.numeric(var)) {
        cols <- var
    } else {
        cols <- match (var, col.names)
    }

    if (length(var) == 1) {
        if (type.cols[cols] == 0) {
            return (vals)
        }

        f <- match (cols, factor.cols)
        return (match (vals, factor.levels[[f]]))
    } else {
        if (all(type.cols[cols] == 0)) {
            return (vals)
        }

        fmap <- match (cols, factor.cols)

        if (is.data.frame(vals)) {
            cmap <- match (col.names[cols], colnames(vals))
        }

        out <- matrix(nrow=nrow(vals), ncol=length(var))
        for (i in 1:length(var)) {
            if (is.na(fmap[i])) {
                out[, i] <- vals[, cmap[i]]
            } else {
                out[, i] <- match(vals[, cmap[i]], factor.levels[[fmap[i]]])
            }
        }
        return (out)
    }
},
filter_range = function (start, end) {
    "Only include specified rows. Note that start and end are relative to all rows in the big.matrix, filtered or otherwise"
    if (empty) { return() }
    profile ("start", "filter_range")
    if (start > 1) {
        bm[1:(start-1), filtercol] <<- 0
    }
    if (end < nrow(bm)) {
        bm[(end+1):nrow(bm), filtercol] <<- 0
    }
    filtered <<- TRUE
    profile ("stop", "filter_range")
},
filter_rows = function (rows) {
    "Only include specified numeric rows. Note that rows refer to all rows in the big.matrix, filtered or otherwise"
    if (empty) { return() }
    profile ("start", "filter_rows")
    bm[, tmpcol] <<- 0
    bm[rows, tmpcol] <<- 1

    bm[, filtercol] <<- bm[, filtercol] * bm[, tmpcol]
    empty <<- sum(bm[, filtercol]) == 0
    filtered <<- TRUE
    profile ("stop", "filter_rows")
},
filter_vector = function (rows) {
    "Only include these rows (given as a vector of TRUE/FALSE values). Note that this applies to all rows in the big.matrix, filtered or otherwise"
    if (empty) { return() }
    profile ("start", "filter_vector")
    bm[, filtercol] <<- bm[, filtercol] * rows
    empty <<- sum(bm[, filtercol]) == 0
    filtered <<- TRUE
    profile ("stop", "filter_vector")
},
free_col = function (cols, update=FALSE) {
    "Free specified (numeric) column and optionally update cluster"
    if (grouped) {
        if (any(cols %in% group.cols)) {
            stop ("Attempted to drop columns currently in use by grouping")
        }
    }
    if (any(cols == filtercol)) {
        stop ("Attempted to drop filter column")
    } else if (any(cols == groupcol)) {
        stop ("Attempted to drop group ID column")
    } else if (any(cols == tmpcol)) {
        stop ("Attempted to drop tmp column")
    }

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
    }
    col.names[cols] <<- NA
    type.cols[cols] <<- 0
    order.cols[cols] <<- 0
    if (update) {
        cluster_export ("cols", ".cols")
        cluster_eval ({
            .local$free_col (.cols, update=FALSE)
            NULL
        })
    }
},
get_data = function (i=NULL, j=NULL, nsa=NULL, drop=TRUE) {
    "Retrieve given rows (i), columns (j). drop=TRUE with 1 column will return a vector, otherwise a standard data.frame. If no strings attached mode is enabled, this will only return a vector or a matrix"
    if (is.null(i)) {
        rowslice <- NULL
    } else {
        rowslice <- i
    }

    if (is.null(nsa)) {
        nsa <- nsamode
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
                filtrows <- filtrows[rowslice]
            } else {
                if (max(rowslice) > nrow(bm)) {
                    stop (sprintf("Invalid row reference: %d > %d", max(rowslice), nrow(bm)))
                }
                filtrows <- rowslice
            }
        } else if (is.logical(rowslice)) {
            if (filtered) {
                filtrows <- bm[, filtercol] == 1
                if (sum(filtrows) %% length(rowslice) != 0) {
                    stop ("Number of available rows needs to be an exact multiple of rowslice length")
                }
                filtrows <- filtrows[rowslice]
            } else {
                if (nrow(bm) %% length(rowslice) != 0) {
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
            out <- cbind (data.frame(out, stringsAsFactors = FALSE), data.frame(o, stringsAsFactors = FALSE))
        }
    }

    if (!drop && length(cols) == 1) {
        out <- data.frame(out, stringsAsFactors = FALSE)
    }

    if (length(cols) > 1 || !drop) {
        colnames (out) <- col.names[cols]
    }

    return (out)
},
group_cache_attach = function (descres) {
    "Attach data frame to group_cache"
    group_cache <<- bigmemory.sri::attach.resource(descres)
},
group_restrict = function (grpid=NULL) {
    "Restricts data to only specified group ID. If NULL, returns to non-restricted."
    if (!grouped || length(group) == 0) {
        stop ("group_restrict may only be used on grouped data")
    }
    if (is.null (grpid)) {
        bm <<- savestate[[1]]
        bindenv <<- savestate[[2]]
        empty <<- savestate[[3]]
        savestate <<- list()
        return()
    }
    if (length(savestate) == 0) {
        savestate <<- list(bm, bindenv, empty)
    } else {
        stop ("Attempted to call group_restrict more than once")
    }

    bindenv <<- groupenv[[which (group == grpid)]]
    if (group_cache[grpid, 1] <= group_cache[grpid, 2] && group_cache[grpid, 1] > 0) {
        bm <<- submatrix (group_cache[grpid, 1], group_cache[grpid, 2])
    } else {
        bm <<- NA_class_("big.matrix")
        empty <<- TRUE
    }
    empty <<- group_cache[grpid, 3] == 0

    return ()
},
local_subset = function (first, last) {
    "Applies sub.big.matrix to bm"
    if (empty) { return() }
    first <<- first
    last <<- last
    bm <<- submatrix (first, last)
},
partition_even = function (extend=FALSE) {
    "Partitions data evenly across cluster, irrespective of grouping boundaries"
    if (empty) { return() }
    N <- length(cls)

    profile ("start", "partition_even")

    if (grouped) {
        destroy_grouped ()
    }
    grouped <<- group_partition <<- FALSE

    if (last == 0) {
        cluster_eval ({
            if (exists(".local")) {
                .local$empty <- TRUE
                .local$grouped <- .local$group_partition <- FALSE
            }
            NULL
        })
        profile ("stop", "partition_even")
        return()
    }

    nr <- distribute (last, N)
    if (last < N) {
        nr[nr != 0] <- 1:last
        cluster_export_each ("nr", ".first")
        cluster_export_each ("nr", ".last")
    } else {
        .last <- cumsum(nr)
        .first <- c(0, .last)[1:N] + 1
        if (extend) {
            .last <- .last + 1
            .last[N] <- last
        }
        cluster_export_each (".first")
        cluster_export_each (".last")
    }

    cluster_eval ({
        .local$grouped <- FALSE
        .local$group_partition <- FALSE

        .local$empty <- (.last < .first || .last == 0)
        if (!.local$empty) {
            .local$local_subset (.first, .last)
        }
        NULL
    })

    profile ("stop", "partition_even")
    return()
},
profile = function (action=NULL, name=NULL) {
    "Profiling function: action may be start or stop. If no parameters, this returns a data.frame of profiling timings"
    if (!profiling) {
        return(data.frame())
    }

    if (is.null(action)) {
        if (!slave) {
            cluster_profile ()
        }

        if (length(profile_names) == 0) {
            return (data.frame())
        }

        if (is.null(name)) {
            m <- order(profile_names)
        } else {
            m <- match (sort(name), profile_names)
        }

        return (data.frame(Profile=profile_names[m],
                           System=profile_sys[m],
                           User=profile_user[m],
                           Real=profile_real[m],
                           stringsAsFactors = FALSE))
    } else if (action == "start") {
        m <- match (name, profile_names)
        if (is.na(m)) {
            profile_names <<- c(profile_names, name)
            profile_ruser <<- c(profile_ruser, 0)
            profile_rsys <<- c(profile_rsys, 0)
            profile_rreal <<- c(profile_rreal, 0)
            profile_user <<- c(profile_user, 0)
            profile_sys <<- c(profile_sys, 0)
            profile_real <<- c(profile_real, 0)
            m <- length(profile_names)
        }

        res <- proc.time()
        profile_ruser[m] <<- res[1][[1]]
        profile_rsys[m] <<- res[2][[1]]
        profile_rreal[m] <<- res[3][[1]]
    } else if (action == "stop") {
        res <- proc.time()

        m <- match (name, profile_names)

        user.diff <- res[1][[1]] - profile_ruser[m]
        sys.diff <- res[2][[1]] - profile_rsys[m]
        real.diff <- res[3][[1]] - profile_rreal[m]

        profile_user[m] <<- profile_user[m] + user.diff
        profile_sys[m] <<- profile_sys[m] + sys.diff
        profile_real[m] <<- profile_real[m] + real.diff
    }
    return (invisible(res))
},
profile_import = function (prof) {
    "Adds totals from provided profile to this data frame's profiling data"
    if (!profiling) { return() }
    if (nrow(prof) == 0) { return() }
    m <- match (prof$Profile, profile_names)
    if (any(is.na(m))) {
        profile_names <<- c(profile_names, prof$Profile[is.na(m)])
        len <- sum(is.na(m))
        profile_sys <<- c(profile_sys, rep(0, len))
        profile_user <<- c(profile_user, rep(0, len))
        profile_real <<- c(profile_real, rep(0, len))
        profile_rsys <<- c(profile_rsys, rep(0, len))
        profile_ruser <<- c(profile_ruser, rep(0, len))
        profile_rreal <<- c(profile_rreal, rep(0, len))
        m <- match (prof$Profile, profile_names)
    }
    profile_sys[m] <<- profile_sys[m] + prof$System
    profile_user[m] <<- profile_user[m] + prof$User
    profile_real[m] <<- profile_real[m] + prof$Real
},
reattach_slave = function (descres) {
    "Used for nodes to reattach to a specified shared memory object"
    nm <- names(descres)
    for (i in 1:length(descres)) {
        field(nm[i], descres[[i]])
    }

    bm.master <<- bm <<- bigmemory::attach.big.matrix(desc.master)
    slave <<- TRUE
    cls <<- NA_class_("SOCKcluster")
},
rebuild_grouped = function () {
    "Executes destroy_grouped(), followed by build_grouped()"
    destroy_grouped()
    build_grouped()
},
row_names = function () {
    "Returns some entirely arbitrary row names"
    if (empty) {
        return(character(0))
    }
    if (filtered) {
        return (seq_len(sum(bm[, filtercol] == 1)))
    } else {
        return (seq_len(nrow(bm)))
    }
},
set_data = function (i=NULL, j=NULL, value, nsa=NULL) {
    "Set data in given rows (i) and columns (j). If in no strings attached mode, then value must be entirely numeric"
    if (is.null(i)) {
        rowslice <- NULL
    } else {
        rowslice <- i
    }

    if (is.null(nsa)) {
        nsa <- nsamode
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
    } else {
        # [i, ] <-
        # [] <-
        cols <- which (order.cols > 0)
    }

    if (is.null(rowslice)) {
        if (filtered) {
            filtrows <- bm[, filtercol] == 1
            nr <- sum(filtrows)
        } else {
            nr <- nrow(bm)
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
                filtrows <- filtrows[rowslice]
            } else {
                nr <- nrow(bm)
                if (max(rowslice) > nr) {
                    stop (sprintf("Invalid row reference: %d > %d", max(rowslice), nr))
                }
                filtrows <- rowslice
            }
            nr <- length(filtrows)
        } else if (is.logical(rowslice)) {
            if (filtered) {
                filtrows <- bm[, filtercol] == 1
                nr <- sum(filtrows)
                if (nr %% length(rowslice) != 0) {
                    stop ("Number of available rows needs to be an exact multiple of rowslice length")
                }
                filtrows <- filtrows[rowslice]
            } else {
                nr <- nrow(bm)
                if (nr %% length(rowslice) != 0) {
                    stop ("Number of available rows needs to be an exact multiple of rowslice length")
                }
                filtrows <- rowslice
            }
            nr <- sum(filtrows)
        }
    }

    dims <- dim(value)
    if (!is.null(dims)) {
        if (dims[1] != nr) {
            stop (sprintf("replacement data has %d rows to replace %d", dims[1], nr))
        }
        if (dims[2] != length(cols)) {
            stop (sprintf("replacement data has %d cols to replace %d", dims[2], length(cols)))
        }
        if ("data.frame" %in% class(value) && nsa) {
            stop ("data.frame not allowed as replacement when in NSA mode")
        }
    } else {
        if (length(value) != 1 && length(value) != nr) {
            stop (sprintf("replacement data has %d rows to replace %d", length(value), nr))
        }
    }

    if (is.null(i) && is.null(j)) {
        # [] <-
        if (is.null(dims)) {
            stop ("replacement data needs to be specified as a matrix or a data.frame")
        }

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
    } else if (is.null(i)) {
        # [, j] <-
        # [j] <-

        if (is.null(dims)) {
            if (length(cols) > 1 && length(value) != 1) {
                stop ("replacement data needs to be specified as a matrix or a data.frame")
            }
        }

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
        if (is.null(dims)) {
            if (length(cols) > 1 && length(value) != 1) {
                stop ("replacement data needs to be specified as a matrix or a data.frame")
            }
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
                bm[, cols] <<- factor_map(cols, value)
            } else {
                bm[filtrows, cols] <<- factor_map(cols, value)
            }
        }
    }
    invisible (value)
},
show = function (max.row=10) {
    "Displays content of data frame; use max.row=0 to not limit number of rows displayed"
    if (nullframe) {
        stop ("Data frames with no rows/columns allocated not supported")
    }

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
    pc.names <- nchar(col.names)
    pc.nb <- pc.names > pc
    pc.nb[is.na(pc.nb)] <- FALSE
    pc[pc.nb] <- pc.names[pc.nb]

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
        if (cluster_running()) {
            .self$calc_group_sizes (delay=FALSE)
        }
        cat (sprintf ("Groups: %d\n", group_max))
        gs <- sprintf ("Group sizes: %s\n", paste(group_cache[, 3], collapse=", "))
        if (nchar(gs) >= 80) {
            cat(sprintf ("Group sizes: median %.1f (IQR %.0f-%.0f, range %.0f-%.0f)\n",
                         median(group_cache[, 3]), quantile(group_cache[, 3], 0.25),
                         quantile(group_cache[, 3], 0.75), min(group_cache[, 3]),
                         max(group_cache[, 3])))
        } else {
            cat (gs)
        }

    }
    if (!cluster_running()) {
        cat ("Cluster not currently running\n")
    } else if (group_partition) {
        res <- cluster_eval ({
            if (.local$empty) {
                .res <- 0
            } else {
                .res <- length(.local$group)
            }
            .res
        })
        res <- do.call (c, res)
        cat (sprintf ("Group partioned over %d clusters\n", length(cls)))
        cat (sprintf ("Groups per cluster: %s\n", paste(res, collapse=", ")))
    } else {
        res <- cluster_eval ({
            if (.local$empty) {
                .res <- 0
            } else {
                .res <- nrow(.local$bm)
            }
            .res
        })
        cat (sprintf ("\nData partitioned over %d clusters\n", length(cls)))
        if (grouped && length(cls) > 1) {
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
sort = function (decreasing=FALSE, dots=NULL, cols=NULL, with.group=TRUE) {
    "Sorts data by specified (numeric) columns or by translating from a lazy_dots object. with.group is used to ensure that the sort is by grouping columns first to ensure contiguity"
    if (empty) { return() }
    profile ("start", "sort")
    if (is.null(cols)) {
        namelist <- names (dots)
        cols <- match(namelist, col.names)
        if (any(is.na(cols))) {
            stop (.p("Undefined column(s): ", paste0(namelist[is.na(cols)], collapse=", ")))
        }
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
    bm_mpermute (bm, cols=cols, decreasing=decreasing)
    profile ("stop", "sort")
},
update_fields = function (fieldnames) {
    "Update specified cluster data frames' field names to be the same as this one's"
    profile ("start", "update_fields")
    for (.fieldname in fieldnames) {
        .fieldval <- .self$field(name=.fieldname)
        cluster_export (c(".fieldname", ".fieldval"))
        cluster_eval({
            .local$field (name = .fieldname, value = .fieldval)
            NULL
        })
    }
    profile ("stop", "update_fields")
},
submatrix = function (a, b) {
    "Returns a sub.big.matrix between specified rows (a:b)"
    sub.big.matrix (desc.master, firstRow=a, lastRow=b)
}
))

#' Data access methods for Multiplyr
#'
#' @param x Data frame
#' @param i Rows
#' @param j Columns
#' @param ... Additional parameters
#' @param drop Logical: whether to reduce a 1 column data frame result to a vector
#' @param value Value to set
#'
#' @rdname Multiplyr-methods
#' @name Multiplyr-methods
NULL

#' Get data
#'
#' @rdname Multiplyr-methods
#' @aliases [,Multiplyr,ANY,ANY,ANY-method
#' @method [ Multiplyr
#' @docType methods
#' @export
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


#' Set data
#'
#' @rdname Multiplyr-methods
#' @aliases [<-,Multiplyr,ANY,ANY-method
#' @method [<- Multiplyr
#' @docType methods
#' @export
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

#' Coerce Multiplyr object to be a standard data.frame
#'
#' @rdname Multiplyr-methods
#' @aliases as.data.frame,Multiplyr-method
#' @method as.data.frame Multiplyr
#' @docType methods
#' @export
setMethod ("as.data.frame", "Multiplyr", function (x) {
    x[]
})

#' List containing row and column names
#'
#' @rdname Multiplyr-methods
#' @aliases dimnames,Multiplyr-method
#' @method dimnames Multiplyr
#' @docType methods
#' @export
setMethod ("dimnames", "Multiplyr", function (x) {
    list(row.names(x), names(x))
})

#' Column names
#'
#' @rdname Multiplyr-methods
#' @aliases names,Multiplyr-method
#' @method names Multiplyr
#' @docType methods
#' @export
setMethod("names", "Multiplyr", function(x) {
    m <- x$order.cols > 0
    (x$col.names[m])[order(x$order.cols[m])]
})

#' Row names
#'
#' @rdname Multiplyr-methods
#' @method row.names Multiplyr
#' @aliases row.names,Multiplyr-method
#' @docType methods
#' @export
setMethod("row.names", "Multiplyr", function (x) {
    x$row_names()
})

