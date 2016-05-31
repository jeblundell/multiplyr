# Operations on Multiplyr objects

#' Add a new column with row names
#'
#' @family column manipulations
#' @param .self Data frame
#' @param var Optional name of column
#' @return Data frame
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=rnorm(100), alloc=1, cl=2)
#' dat %>% add_rownames()
#' dat %>% shutdown()
#' }
add_rownames <- function (.self, var="rowname") {
    if (!is(.self, "Multiplyr")) {
        stop ("add_rownames operation only valid for Multiplyr objects")
    }

    col <- .self$alloc_col (var)
    .self$bm.master[, col] <- 1:nrow(.self$bm.master)

    return (.self)
}

#' @rdname arrange
#' @export
arrange_ <- function (.self, ..., .dots) {
    #This works on the presumption that factors have levels
    #sorted already
    if (!is(.self, "Multiplyr")) {
        stop ("arrange operation only valid for Multiplyr objects")
    }
    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0 || .self$empty) {
        return (.self)
    }

    sort.names <- c()
    sort.desc <- c()
    for (i in 1:length(.dots)) {
        if (length(.dots[[i]]) == 1) {
            sort.names <- c(sort.names, as.character(.dots[[i]]))
            sort.desc <- c(sort.desc, FALSE)
        } else {
            fn <- as.character(.dots[[i]][[1]])
            if (length(.dots[[i]]) == 2 && fn == "desc") {
                sort.names <- c(sort.names, as.character(.dots[[i]][[2]]))
                sort.desc <- c(sort.desc, TRUE)
            } else {
                stop (paste0 ("arrange can't handle sorting expression: ", deparse(.dots[[i]])))
            }
        }
    }

    cols <- match(sort.names, .self$col.names)
    if (any(is.na(cols))) {
        stop (sprintf("Undefined columns: %s", paste0(sort.names[is.na(cols)], collapse=", ")))
    }
    .self$sort(decreasing=sort.desc, cols=cols)
    return (.self)
}

#' @rdname define
#' @export
define_ <- function (.self, ..., .dots) {
    if (!is(.self, "Multiplyr")) {
        stop ("arrange operation only valid for Multiplyr objects")
    }
    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No column names specified")
    }
    nm <- names(.dots)
    if (any(nm %in% .self$col.names)) {
        stop (sprintf("Columns already defined: %s", paste0(.self$col.names[.self$col.names %in% nm], collapse=", ")))
    }
    for (i in 1:length(.dots)) {
        col <- .self$alloc_col (nm[i])
        if (nm[i] != as.character(.dots[[i]])) { #x=y
            #Set type based on template
            tpl <- as.character(.dots[[i]])
            tcol <- match (tpl, .self$col.names)
            .self$type.cols[col] <- .self$type.cols[tcol]

            #Copy levels from template
            if (.self$type.cols[tcol] > 0) {
                f <- match (tcol, .self$factor.cols)
                .self$factor.cols <- c(.self$factor.cols, col)
                .self$factor.levels <- append(.self$factor.levels,
                                              list(.self$factor.levels[[f]]))
            }
        }
    }
    .self$update_fields (c("col.names", "type.cols", "factor.cols", "factor.levels"))
    return (.self)
}

#' @rdname distinct
#' @export
distinct_ <- function (.self, ..., .dots, auto_compact = NULL) {
    if (!is(.self, "Multiplyr")) {
        stop ("distinct operation only valid for Multiplyr objects")
    }
    if (.self$empty) { return() }
    .dots <- dotscombine (.dots, ...)

    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }

    N <- length (.self$cls)

    if (length(.dots) > 0) {
        namelist <- names (.dots)
        .cols <- match(namelist, .self$col.names)
        if (any(is.na(.cols))) {
            stop (sprintf("Undefined columns: %s", paste0(namelist[is.na(.cols)], collapse=", ")))
        }
    } else {
        .cols <- .self$order.cols > 0
        .cols <- (1:length(.cols))[.cols]
    }

    if (.self$grouped) {
        idx <- match (.self$groupcol, .cols)
        if (!is.na(idx)) {
            .cols <- .cols[-idx]
        }
        .cols <- c(.self$groupcol, .cols)
    }

    if (nrow(.self$bm) == 1) {
        return (.self)
    }

    .self$sort (decreasing=FALSE, cols=.cols)
    if (N == 1) {
        if (nrow(.self$bm) == 2) {
            .self$bm[2, .self$filtercol] <- ifelse (
                all(.self$bm[1, .cols] == .self$bm[2, .cols]), 0, 1)
            return (.self)
        }
        sm1 <- bigmemory.sri::attach.resource(sm_desc_comp (.self, 1))
        sm2 <- bigmemory.sri::attach.resource(sm_desc_comp (.self, 2))
        if (length(.cols) == 1) {
            breaks <- which (sm1[,.cols] != sm2[,.cols])
        } else {
            breaks <- which (!apply (sm1[,.cols] == sm2[,.cols], 1, all))
        }
        breaks <- c(0, breaks) + 1

        .self$filter_rows (breaks)

        return (.self)
    }

    .self$cluster_export (c(".cols"))

    # (0) If partitioned by group, temporarily repartition evenly
    rg_grouped <- .self$grouped
    rg_partition <- .self$group_partition
    rg_cols <- .self$group.cols

    if (rg_partition) {
        .self$partition_even()
    }

    # (1) determine local distinct rows
    trans <- .self$cluster_eval ({
        if (.local$empty) {
            .res <- NA
        } else {
            if (nrow(.local$bm) == 1) {
                .breaks <- 1
                .res <- 1
            } else if (nrow(.local$bm) == 2) {
                i <- ifelse (all(.local$bm[1, .cols] ==
                                     .local$bm[2, .cols]), 1, 2)
                .breaks <- 1:i
                .res <- i
            } else {
                .sm1 <- bigmemory.sri::attach.resource(sm_desc_comp (.local, 1))
                .sm2 <- bigmemory.sri::attach.resource(sm_desc_comp (.local, 2))

                if (length(.cols) == 1) {
                    .breaks <- which (.sm1[,.cols] != .sm2[,.cols])
                    .breaks <- .breaks + 1
                    .res <- .local$last
                } else {
                    if (nrow(.local$bm) == 1) {
                        .breaks <- 1
                        .res <- 1
                    } else if (nrow(.local$bm) == 2) {
                        i <- ifelse (all(.local$bm[1, .cols] ==
                                             .local$bm[2, .cols]), 1, 2)
                        .breaks <- 1:i
                        .res <- i
                    } else {
                        if (length(.cols) == 1) {
                            .breaks <- which (.sm1[,.cols] != .sm2[,.cols])
                        } else {
                            .breaks <- which (!apply (.sm1[,.cols] == .sm2[,.cols], 1, all))
                        }
                        rm (.sm1, .sm2)
                        .breaks <- .breaks + 1
                        .res <- .local$last
                    }
                }
            }
        }
        .res
    })

    # (2) work out if there's a group change between local[1] and local[2] etc.
    trans <- do.call (c, trans)
    trans <- trans[-length(trans)] #last row not a transition
    tg <- test_transition (.self, .cols, trans)
    tg <- c(TRUE, tg) #first node is a pseudo-transition

    # (3) set breaks=1 for all where there's a transition
    .self$cluster_export_each ("tg", ".tg")
    .self$cluster_eval ({
        if (!.local$empty) {
            if (.tg) {
                .breaks <- c(1, .breaks)
            }
        }
        NULL
    })

    # (4) filter at breaks
    .self$cluster_eval ({
        if (!.local$empty) {
            .local$filter_rows (.breaks)
        }
        NULL
    })
    .self$filtered <- TRUE

    .self$grouped <- rg_grouped
    .self$group_partition <- rg_partition
    .self$group.cols <- rg_cols

    if (auto_compact) {
        .self$compact()
        .self$calc_group_sizes()
        return (.self)
    }

    .self$calc_group_sizes()

    # Repartition by group if appropriate
    if (rg_partition) {
        return (partition_group_(.self))
    } else {
        if (.self$grouped) {
            .self$rebuild_grouped()
        }
        return (.self)
    }

    return(.self)
}

#' @rdname filter
#' @export
filter_ <- function (.self, ..., .dots, auto_compact = NULL) {
    if (!is(.self, "Multiplyr")) {
        stop ("filter operation only valid for Multiplyr objects")
    }
    if (.self$empty) {
        return (.self)
    }

    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No filtering criteria specified")
    }

    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }

    .self$cluster_export (c(".dots"))
    .self$cluster_eval ({
        if (!.local$empty) {
            if (.local$grouped) {
                for (.g in .local$group) {
                    if (.local$group_cache[.g, 3] > 0) {
                        for (.i in 1:length(.dots)) {
                            .local$group_restrict (.g)
                            .res <- dotseval (.dots[.i], .local$envir())
                            .local$filter_vector (.res[[1]])
                            .local$group_restrict ()
                        }
                    }
                }
            } else {
                for (.i in 1:length(.dots)) {
                    .res <- dotseval (.dots[.i], .local$envir())
                    .local$filter_vector (.res[[1]])
                }
            }
        }
        NULL
    })
    .self$filtered <- TRUE

    if (auto_compact) {
        .self$compact()
    }
    .self$calc_group_sizes()

    return (.self)
}

#' @rdname group_by
#' @export
group_by_ <- function (.self, ..., .dots, .cols=NULL, auto_partition=NULL) {
    if (!is(.self, "Multiplyr")) {
        stop ("group_by operation only valid for Multiplyr objects")
    }

    if (.self$empty) { return (.self) }

    if (is.null(.cols)) {
        .dots <- dotscombine (.dots, ...)
        namelist <- names (.dots)

        .cols <- match(namelist, .self$col.names)
        if (any(is.na(.cols))) {
            stop (sprintf("Undefined columns: %s", paste0(namelist[is.na(.cols)], collapse=", ")))
        }
    }

    if (length(.cols) == 0) {
        stop ("No grouping columns specified")
    }

    if (is.null(auto_partition)) {
        auto_partition <- .self$auto_partition
    }
    N <- length(.self$cls)

    .self$sort (decreasing=FALSE, cols=.cols, with.group=FALSE)

    .self$group.cols <- .cols
    .self$grouped <- TRUE
    .self$group_sizes_stale <- FALSE

    if (nrow(.self$bm) == 1) {
        .self$bm[, .self$groupcol] <- 1
        .self$group_cache <- bigmemory::big.matrix (nrow=1, ncol=3)
        .self$group_cache[1, ] <- c(1, 1, 1)
        .self$group_max <- 1
        return (.self)
    }

    #(0) If partitioned by group, temporarily repartition evenly
    regroup_partition <- .self$group_partition

    #(1) Find all breaks locally, but extend each "last" by 1 to catch
    #    transitions
    #(2) Add (first-1) to each break
    #(3) Return # breaks in each cluser (b.len)
    .self$partition_even (extend=TRUE)
    .self$cluster_export (".cols")
    res <- .self$cluster_eval ({
        if (nrow(.local$bm) == 1) {
            .breaks <- c()
        } else if (nrow(.local$bm) == 2) {
            .breaks <- ifelse (all(.local$bm[1, .cols] ==
                             .local$bm[2, .cols]), c(), .local$first+1)
        } else {
            sm1 <- bigmemory.sri::attach.resource (sm_desc_comp (.local, 1))
            sm2 <- bigmemory.sri::attach.resource (sm_desc_comp (.local, 2))
            if (length(.cols) == 1 || nrow(.local$bm) == 1) {
                .breaks <- which (sm1[, .cols] != sm2[, .cols])
            } else {
                .breaks <- which (!apply (sm1[, .cols] == sm2[, .cols], 1, all))
            }
            if (.local$first == 1) {
                .breaks <- c(0, .breaks)
            }
            .breaks <- .breaks + .local$first
        }
        .length <- length(.breaks)
    })
    b.len <- do.call (c, res)
    G.count <- sum(b.len)

    #(4) Allocate group_cache with nrow=sum(b.len)+1
    .self$group_cache <- bigmemory::big.matrix (nrow=G.count, ncol=3)

    #(5) Pass offset into group_cache[, 2] to each cluster node
    b.off <- c(0, cumsum(b.len))[-(length(b.len)+1)] + 1
    gcdesc <- bigmemory.sri::describe (.self$group_cache)
    .self$cluster_export_each ("b.off", ".offset")
    .self$cluster_export ("gcdesc", ".gcdesc")

    #(6) Each cluster node constructs group_cache[, 2]
    .self$cluster_eval ({
        .local$group_cache_attach (.gcdesc)
        if (.length > 0) {
            .local$group_cache[.offset:(.offset+.length-1), 1] <- .breaks
            #.local$group_cache <- bigmemory.sri::attach.resource(sm_desc_subset(.gcdesc, .offset, .offset+.length-1))
            #.local$group_cache[, 1] <- .breaks
        }
        NULL
    })

    #(7) Fill in the blanks
    if (G.count > 1) {
        .self$group_cache[1:(G.count-1), 2] <- .self$group_cache[2:G.count, 1] - 1
    }
    .self$group_cache[G.count, 2] <- .self$last

    #(8) Calculate group sizes
    #(9) Assign group IDs
    #FIXME: make parallel (use calc_group_sizes?)
    .self$group_cache[, 3] <- (.self$group_cache[, 2] - .self$group_cache[, 1]) + 1
    .self$group_max <- G.count
    for (i in 1:G.count) {
        .self$bm[.self$group_cache[i, 1]:.self$group_cache[i, 2], .self$groupcol] <- i
    }

    #Needed to allow $group_restrict on master node
    .self$group <- 1:G.count

    if (auto_partition && !regroup_partition) {
        .self$group_partition <- TRUE
        regroup_partition <- TRUE
    }

    # Repartition by group if appropriate
    if (regroup_partition) {
        .self$grouped <- TRUE
        return (partition_group_(.self))
    } else {
        .self$rebuild_grouped()
        .self$update_fields ("grouped")
        return (.self)
    }
}

#' Return size of groups
#'
#' This function is used to find the size of groups in a Multiplyr data frame
#'
#' @param .self Data frame
#' @return Group sizes
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), length.out=100))
#' dat %>% group_by (G)
#' group_sizes (dat)
#' dat %>% shutdown()
#' }
group_sizes <- function (.self) {
    if (!is(.self, "Multiplyr")) {
        stop ("group_sizes operation only valid for Multiplyr objects")
    }
    if (!.self$grouped) {
        stop ("group_sizes may only be used after group_by")
    }
    .self$calc_group_sizes (delay=FALSE)
    .self$group_cache[, 3]
}

#' Return number of groups
#'
#' @family utility functions
#' @param .self Data frame
#' @return Number of groups in data frame
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(1:4, each=25), cl=2)
#' dat %>% group_by (G)
#' n_groups (dat)
#' dat %>% shutdown()
#' }
n_groups <- function (.self) {
    if (!is(.self, "Multiplyr")) {
        stop ("n_groups operation only valid for Multiplyr objects")
    }
    if (!.self$grouped) {
        return (0)
    } else {
        return (nrow(.self$group_cache))
    }
}

#' @rdname mutate
#' @export
mutate_ <- function (.self, ..., .dots) {
    if (!is(.self, "Multiplyr")) {
        stop ("mutate operation only valid for Multiplyr objects")
    }
    if (.self$empty) { return (.self) }

    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No mutation operations specified")
    }

    .resnames <- names(.dots)
    .rescols <- .self$alloc_col (.resnames, update=TRUE)
    if (any(.rescols == .self$group.cols)) {
        if (.self$grouped) {
            stop("mutate on a group column is not permitted")
        } else {
            .self$group.cols <- numeric(0)
        }
    }

    .self$cluster_export (c(".resnames", ".rescols", ".dots"))
    .self$cluster_eval ({
        if (!.local$empty) {
            if (.local$grouped) {
                for (.g in .local$group) {
                    if (.local$group_cache[.g, 1] > 0) {
                        for (.i in 1:length(.dots)) {
                            .local$group_restrict (.g)
                            .res <- dotseval (.dots[.i], .local$envir())
                            .local$set_data (, .rescols[.i], .res[[1]])
                            .local$group_restrict ()
                        }
                    }
                }
            } else {
                for (.i in 1:length(.dots)) {
                    .res <- dotseval (.dots[.i], .local$envir())
                    .local$set_data (, .rescols[.i], .res[[1]])
                }
            }
        }
        NULL
    })

    return (.self)
}

#' No strings attached mode
#'
#' This function may be used to set or unset whether a data frame is in no
#' strings attached mode, potentially speeding up various operations.
#'
#' This function will place a data frame in no strings attached mode, which
#' disables translation of character values to and from numeric representation.
#' This allows for much faster calculations.
#'
#' @family data manipulations
#' @param .self Data frame
#' @param enabled TRUE to enable, FALSE to disable. Defaults to TRUE.
#' @return Data frame
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (G=rep(c("A", "B", "C", "D"), length.out=100))
#' dat %>% nsa () %>% mutate (G=max(G)) %>% nsa(FALSE)
#' dat %>% shutdown()
#' }
nsa <- function (.self, enabled=TRUE) {
    if (!is(.self, "Multiplyr")) {
        stop ("nsa operation only valid for Multiplyr objects")
    }
    if (.self$nsamode && enabled) {
        warning ("nsa() operation applied when data frame already in NSA-mode")
    } else if (!.self$nsamode && !enabled) {
        warning ("nsa(FALSE) operation applied when data frame already not in NSA-mode")
    }
    .self$nsamode <- enabled
    .self$update_fields ("nsamode")
    return (.self)
}

#' Partition data evenly amongst cluster nodes
#'
#' This function results in data being repartitioned evenly across cluster nodes,
#' ignoring any grouping variables.
#'
#' @family cluster functions
#' @param .self Data frame
#' @return Data frame
#' @export
partition_even <- function (.self) {
    if (!is(.self, "Multiplyr")) {
        stop ("partition_even operation only valid for Multiplyr objects")
    }
    .self$partition_even ()
    .self$group_partition <- FALSE
    .self$update_fields ("group_partition")
    return(.self)
}

#' @rdname partition_group
#' @export
partition_group_ <- function (.self, ..., .dots) {
    if (!is(.self, "Multiplyr")) {
        stop ("partition_group operation only valid for Multiplyr objects")
    }
    .dots <- dotscombine (.dots, ...)

    if (length(.dots) > 0) {
        .self$group_partition <- TRUE
        return (group_by_ (.self, .dots=.dots))
        #group_by_ calls partition_group() on its return
    }
    if (!.self$grouped) {
        stop ("Need to specify grouping factors or apply group_by first")
    }

    N <- length(.self$cls)

    G <- .self$group_cache[, 3]
    if (length(G) == 1) {
        Gi <- distribute (1, N)
        Gi[Gi == 0] <- NA
    } else {
        Gi <- distribute (G, N)
    }

    .self$cluster_export_each ("Gi", ".groups")

    .self$destroy_grouped()
    .self$cluster_profile()

    .self$cluster_eval ({
        if (NA %in% .groups) {
            .local$empty <- TRUE
        }

        if (!.local$empty) {
            .local$group <- .groups
        }

        NULL
    })

    .self$group_partition <- .self$grouped <- TRUE
    .self$update_fields (c("grouped", "group_partition"))
    .self$build_grouped ()

    return (.self)
}

#' @rdname reduce
#' @export
reduce_ <- function (.self, ..., .dots, auto_compact = NULL) {
    if (!is(.self, "Multiplyr")) {
        stop ("reduce operation only valid for Multiplyr objects")
    }
    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No reduce operations specified")
    }

    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }

    avail <- which (substr (.self$col.names, 1, 1) != ".")
    if (.self$grouped) {
        avail <- avail[!(avail %in% .self$group.cols)]
    }
    avail <- sort(c(avail, which (is.na(.self$col.names))))

    newnames <- names(.dots)
    if (length(newnames) > length(avail)) {
        stop ("Insufficient free columns available")
    }
    newcols <- avail[1:length(newnames)]

    if (!.self$empty) {
        if (.self$grouped) {
            for (g in 1:.self$group_max) {
                .self$group_restrict (g)
                res <- dotseval (.dots, .self$envir())
                len <- 0
                for (i in 1:length(res)) {
                    .self$bm[, newcols[i]] <- res[[i]]
                    if (length(res[[i]]) > len) {
                        len <- length(res[[i]])
                    }
                }
                .self$bm[, .self$filtercol] <- 0
                .self$bm[1:len, .self$filtercol] <- 1
                .self$filtered <- TRUE
                .self$group_restrict ()
            }
        } else {
            res <- dotseval (.dots, .self$envir())
            len <- 0
            for (i in 1:length(res)) {
                .self$bm[, newcols[i]] <- res[[i]]
                if (length(res[[i]]) > len) {
                    len <- length(res[[i]])
                }
            }
            .self$bm[, .self$filtercol] <- 0
            .self$bm[1:len, .self$filtercol] <- 1
            .self$filtered <- TRUE
        }
    }
    if (auto_compact) {
        .self$compact()
    }

    .self$free_col (avail, update=TRUE)
    .self$alloc_col (newnames, update=TRUE)

    .self$calc_group_sizes()

    return (.self)
}

#' Return to grouped data
#'
#' After a data frame has been grouped and then ungrouped, this function acts
#' as a shorthand (and faster way) to reinstate grouping.
#'
#' @param .self Data frame
#' @param auto_partition Re-partition across cluster after operation
#' @return Data frame
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), length.out=100))
#' dat %>% group_by (G)
#' dat %>% ungroup() %>% regroup()
#' dat %>% summarise (N=length(x))
#' dat %>% shutdown()
#' }
regroup <- function (.self, auto_partition=NULL) {
    if (!is(.self, "Multiplyr")) {
        stop ("regroup operation only valid for Multiplyr objects")
    }
    if (.self$grouped) {
        warning ("regroup attempted on an object that's already grouped")
        return (.self)
    }
    if (length(.self$group.cols) == 0) {
        stop ("regroup may only be used after group_by (and without modifying the group columns)")
    }

    if (is.null(auto_partition)) {
        auto_partition <- .self$auto_partition
    }

    .self$grouped <- TRUE
    .self$update_fields ("grouped")

    if (auto_partition) {
        .self$group_partition <- TRUE
        .self$update_fields ("group_partition")
    }

    .self$build_grouped()

    return (.self)
}

#' @rdname rename
#' @export
rename_ <- function (.self, ..., .dots) {
    if (!is(.self, "Multiplyr")) {
        stop ("rename operation only valid for Multiplyr objects")
    }
    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No renaming operations specified")
    }
    newnames <- names(.dots)
    oldnames <- simplify2array (as.character(.dots))
    m <- match(oldnames, .self$col.names)
    if (any(is.na(m))) {
        stop (sprintf("Undefined columns: %s", paste0(oldnames[is.na(m)], collapse=", ")))
    }
    .self$col.names[m] <- newnames
    .self$update_fields ("col.names")
    return (.self)
}

#' @rdname select
#' @export
select_ <- function (.self, ..., .dots) {
    if (!is(.self, "Multiplyr")) {
        stop ("select operation only valid for Multiplyr objects")
    }
    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No select columns specified")
    }
    coln <- simplify2array (as.character(.dots))
    cols <- match (coln, .self$col.names)
    if (any(is.na(cols))) {
        stop (sprintf("Undefined columns: %s", paste0(coln[is.na(cols)], collapse=", ")))
    }

    .self$order.cols[sort(cols)] <- order(cols)
    #rest set to zero by free_col (del)

    del <- substr(.self$col.names, 1, 1) != "."
    del[is.na(del)] <- FALSE
    del[cols] <- FALSE
    if (.self$grouped) {
        del[.self$group.cols] <- FALSE
    }

    del <- which (del)
    .self$free_col (del, update=TRUE)

    return (.self)
}

#' Shutdown running cluster
#'
#' Theoretically a Multiplyr data frame will have its cluster implicitly
#' shutdown when R's garbage collection kicks in. This function exists to
#' execute it explicitly. This does not affect any of the data.
#'
#' @family cluster functions
#' @param .self Data frame
#' @export
shutdown <- function (.self) {
    if (!is(.self, "Multiplyr")) {
        stop ("shutdown operation only valid for Multiplyr objects")
    } else if (!.self$cluster_running()) {
        warning ("Attempt to shutdown cluster that's already not running")
    }

    .self$cluster_profile ()
    if (.self$grouped) {
        .self$calc_group_sizes(delay=FALSE)
    }
    .self$cluster_stop (only.if.started=FALSE)

    return (.self)
}

#' Select rows by position
#'
#' This function is used to filter out everything except a specified subset of
#' the data. The each parameter is used to change slice's behaviour to filter
#' out all except a specified subset within each group or, if no grouping,
#' within each node.
#'
#' @family row manipulations
#' @param .self Data frame
#' @param rows Rows to select
#' @param start Start of range of rows
#' @param end End of range of rows
#' @param each Apply slice to each cluster/group
#' @param auto_compact Compact data
#' @return Data frame
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25))
#' dat %>% group_by (G)
#' dat %>% slice (1:10, each=TRUE)
#' dat %>% slice (1:10)
#' dat %>% shutdown()
#' }
slice <- function (.self, rows=NULL, start=NULL, end=NULL, each=FALSE, auto_compact=NULL) {
    if (!is(.self, "Multiplyr")) {
        stop ("slice operation only valid for Multiplyr objects")
    } else if (is.null(rows) && (is.null(start) || is.null(end))) {
        stop ("Must specify either rows or start and stop")
    } else if (!is.null(rows) && !(is.null(start) || is.null(end))) {
        stop ("Can either specify rows or start and stop; not both")
    }

    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }

    if (each) {
        if (is.null(rows)) {
            .self$cluster_export (c("start", "end"), c(".start", ".end"))
            .self$cluster_eval ({
                if (!.local$empty) {
                    if (.local$grouped) {
                        for (.g in .local$group) {
                            if (.local$group_cache[.g, 1] > 0) {
                                .local$group_restrict (.g)
                                .local$filter_range (.start, .end)
                                .local$group_restrict()
                            }
                        }
                    } else {
                        .local$filter_range (.start, .end)
                    }
                }
                NULL
            })
        } else if (is.logical(rows)) {
            .self$cluster_export ("rows", ".rows")
            .self$cluster_eval ({
                if (!.local$empty) {
                    if (.local$grouped) {
                        for (.g in .local$group) {
                            if (.local$group_cache[.g, 3] > 0) {
                                .local$group_restrict (.g)
                                .local$filter_vector (.rows)
                                .local$group_restrict ()
                            }
                        }
                    } else {
                        .local$filter_vector (.rows)
                    }
                }
                NULL
            })
        } else {
            .self$cluster_export ("rows", ".rows")
            .self$cluster_eval ({
                if (!.local$empty) {
                    if (.local$grouped) {
                        for (.g in .local$group) {
                            if (.local$group_cache[.g, 3] > 0) {
                                .local$group_restrict (.g)
                                .local$filter_rows (.rows)
                                .local$group_restrict ()
                            }
                        }
                    } else {
                        .local$filter_rows (.rows)
                    }
                }
                NULL
            })
        }
    } else {
        if (is.null(rows)) {
            .self$filter_range (start, end)
        } else if (is.logical(rows)) {
            .self$filter_vector (rows)
        } else {
            .self$filter_rows (rows)
        }
    }

    .self$filtered <- TRUE
    .self$calc_group_sizes()

    if (auto_compact) {
        .self$compact()
    }

    .self
}

#' @rdname summarise
#' @export
summarise_ <- function (.self, ..., .dots, auto_compact = NULL) {
    if (!is(.self, "Multiplyr")) {
        stop ("summarise operation only valid for Multiplyr objects")
    }
    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No summarise operations specified")
    }

    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }

    avail <- which (substr (.self$col.names, 1, 1) != ".")
    if (.self$grouped) {
        avail <- avail[!(avail %in% .self$group.cols)]
    }
    avail <- sort(c(avail, which (is.na(.self$col.names))))

    newnames <- names(.dots)
    if (length(newnames) > length(avail)) {
        stop ("Insufficient free columns available")
    }
    .newcols <- avail[1:length(newnames)]

    .self$cluster_export (c(".dots", ".newcols"))
    .self$cluster_eval ({
        if (!.local$empty) {
            if (.local$grouped) {
                for (.g in .local$group) {
                    if (.local$group_cache[.g, 1] > 0) {
                        .local$group_restrict (.g)
                        .res <- dotseval (.dots, .local$envir())
                        .len <- 0
                        for (.i in 1:length(.res)) {
                            .local$bm[, .newcols[.i]] <- .res[[.i]]
                            if (length(.res[[.i]]) > .len) {
                                .len <- length(.res[[.i]])
                            }
                        }
                        .local$bm[, .local$filtercol] <- 0
                        .local$bm[1:.len, .local$filtercol] <- 1
                        .local$filtered <- TRUE
                        .local$group_restrict()
                    }
                }
            } else {
                .res <- dotseval (.dots, .local$envir())
                .len <- 0
                for (.i in 1:length(.res)) {
                    .local$bm[, .newcols[.i]] <- .res[[.i]]
                    if (length(.res[[.i]]) > .len) {
                        .len <- length(.res[[.i]])
                        .local$bm[, .local$filtercol] <- 0
                        .local$bm[1:.len, .local$filtercol] <- 1
                        .local$filtered <- TRUE
                    }
                }
            }
        }
        NULL
    })
    .self$filtered <- TRUE
    if (auto_compact) {
        .self$compact()
    }

    .self$free_col (avail, update=TRUE)
    .self$alloc_col (newnames, update=TRUE)

    .self$calc_group_sizes()

    return (.self)
}

#' @rdname transmute
#' @export
transmute_ <- function (.self, ..., .dots) {
    if (!is(.self, "Multiplyr")) {
        stop ("transmute operation only valid for Multiplyr objects")
    }

    if (.self$empty) { return (.self) }

    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No mutation operations specified")
    }

    #mutate
    .resnames <- names(.dots)
    .rescols <- .self$alloc_col (.resnames, update=TRUE)
    if (any(.rescols == .self$group.cols)) {
        if (.self$grouped) {
            stop("transmute on a group column is not permitted")
        } else {
            .self$group.cols <- numeric(0)
        }
    }

    .self$cluster_export (c(".resnames", ".rescols", ".dots"))
    .self$cluster_eval ({
        if (!.local$empty) {
            if (.local$grouped) {
                for (.g in .local$group) {
                    if (.local$group_cache[.g, 1] > 0) {
                        for (.i in 1:length(.dots)) {
                            .local$group_restrict (.g)
                            .res <- dotseval (.dots[.i], .local$envir())
                            .local$set_data (, .rescols[.i], .res[[1]])
                            .local$group_restrict ()
                        }
                    }
                }
            } else {
                for (.i in 1:length(.dots)) {
                    .res <- dotseval (.dots[.i], .local$envir())
                    .local$set_data (, .rescols[.i], .res[[1]])
                }
            }
        }
        NULL
    })
    #/mutate

    dropcols <- .self$order.cols > 0
    dropcols[.rescols] <- FALSE
    if (.self$grouped) {
        dropcols[.self$group.cols] <- FALSE
    }
    dropcols <- which (dropcols)

    .self$free_col (dropcols, update=TRUE)
    .self$update_fields (c("col.names", "type.cols", "order.cols"))

    return (.self)
}

#' @rdname undefine
#' @export
undefine_ <- function (.self, ..., .dots) {
    if (!is(.self, "Multiplyr")) {
        stop ("undefine operation only valid for Multiplyr objects")
    }

    .dots <- dotscombine (.dots, ...)
    if (length(.dots) == 0) {
        stop ("No undefine operations specified")
    }

    dropnames <- names (.dots)
    dropcols <- match (dropnames, .self$col.names)
    if (any(is.na(dropcols))) {
        stop (sprintf("Undefined columns: %s", paste0(dropnames[is.na(dropcols)], collapse=", ")))
    }

    .self$free_col (dropcols, update=TRUE)
    return (.self)
}

#' @rdname undefine
#' @export
unselect_ <- undefine_

#' Return data to non-grouped
#'
#' After grouping data with group_by, there may be a need to return to a
#' non-grouped form. Running ungroup() will drop any grouping. This can be
#' reinstated again with regroup().
#'
#' @param .self Data frame
#' @return Data frame
#' @export
ungroup <- function (.self) {
    if (!is(.self, "Multiplyr")) {
        stop ("ungroup operation only valid for Multiplyr objects")
    }
    if (!.self$grouped) {
        warning ("ungroup attempted on an object that's not grouped")
        return (.self)
    }
    .self$destroy_grouped ()
    .self$grouped <- .self$group_partition <- FALSE
    .self$update_fields (c("grouped", "group_partition"))
    .self$partition_even ()
    return (.self)
}

#' @rdname ungroup
#' @export
rowwise <- ungroup

#' @rdname regroup
#' @export
groupwise <- regroup

#' Execute code within a group
#'
#' This is the mainstay of parallel computation for a data frame. This will
#' execute the specified expression within each group. Each group will have a
#' persistent environment, so that variables created in that environment can
#' be referred to by, for example, later calls to summarise. This environment
#' contains active bindings to the columns of that data frame.
#'
#' @family data manipulations
#' @param .self Data frame
#' @param expr Code to execute
#' @return Data frame
#' @export
#' @examples
#' \donttest{
#' dat <- Multiplyr (G = rep(c("A", "B"), each=50),
#'                   m = rep(c(5, 10), each=50),
#'                   alloc=1)
#' dat %>% group_by (G) %>% mutate (x=rnorm(length(m), mean=m))
#' dat %>% within_group ({
#'     mdl <- lm (x ~ 1)
#' })
#' dat %>% summarise (x.mean = coef(mdl)[[1]])
#' dat %>% shutdown()
#' }
within_group <- function (.self, expr) {
    if (!is(.self, "Multiplyr")) {
        stop ("within_group operation only valid for Multiplyr objects")
    }
    if (!.self$grouped) {
        stop ("within_group may only be used after group_by")

    }
    expr <- substitute(expr)
    .self$cluster_export ("expr", ".expr")
    .self$cluster_eval({
        if (!.local$empty) {
            for (.g in .local$group) {
                .local$group_restrict (.g)
                if (!.local$empty) {
                    eval (.expr, envir = .local$envir())
                }
                .local$group_restrict ()
            }
        }
        NULL
    })
    .self
}

#' Execute code within a node
#'
#' This is the mainstay of parallel computation for a data frame. This will
#' execute the specified expression within each node. Each node will have a
#' persistent environment, so that variables created in that environment can
#' be referred to by, for example, later calls to summarise. This environment
#' contains active bindings to the columns of that data frame.
#'
#' @family data manipulations
#' @param .self Data frame
#' @param expr Code to execute
#' @export
within_node <- function (.self, expr) {
    if (!is(.self, "Multiplyr")) {
        stop ("within_node operation only valid for Multiplyr objects")
    }
    expr <- substitute(expr)
    .self$cluster_export ("expr", ".expr")
    .self$cluster_eval({
        if (!.local$empty) {
            eval (.expr, envir = .local$envir())
        }
        NULL
    })
    .self
}
