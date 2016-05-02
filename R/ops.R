# Operations on Multiplyr objects

#' @describeIn arrange
#' @export
arrange_ <- function (.self, ..., .dots) {
    #This works on the presumption that factors have levels
    #sorted already
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .self$sort(decreasing=FALSE, .dots)
}

#' @describeIn define
#' @export
define_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    nm <- names(.dots)
    vars <- .dots2names (.dots)
    for (i in 1:length(.dots)) {
        if (nm[i] == "") { #x
            .self$alloc_col (vars[i])
        } else {           #x=y
            col <- .self$alloc_col (vars[i])
            #Set type based on template
            tpl <- as.character(.dots[[i]]$expr)
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

#' @describeIn distinct
#' @export
distinct_ <- function (.self, ..., .dots, auto_compact = NULL) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)

    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }

    N <- length (.self$cls)

    if (length(.dots) > 0) {
        namelist <- .dots2names (.dots)
        .cols <- match(namelist, .self$col.names)
        .self$sort (decreasing=FALSE, dots=.dots)
    } else {
        .cols <- .self$order.cols > 0
        .cols <- (1:length(.cols))[.cols]
        .self$sort (decreasing=FALSE, cols=.cols)
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
        sm1 <- bigmemory::sub.big.matrix (.self$bm, firstRow=1, lastRow=nrow(.self$bm)-1)
        sm2 <- bigmemory::sub.big.matrix (.self$bm, firstRow=2, lastRow=nrow(.self$bm))
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
    regroup_partition <- .self$group_partition
    if (.self$group_partition) {
        .self$partition_even()
    }

    # (1) determine local distinct rows
    trans <- .self$cluster_eval ({
        if (.local$empty) { return (NA) }
        if (nrow(.local$bm) == 1) {
            .breaks <- 1
            return (1)
        } else if (nrow(.local$bm) == 2) {
            i <- ifelse (all(.local$bm[1, .cols] ==
                                 .local$bm[2, .cols]), 1, 2)
            .breaks <- 1:i
            return (i)
        }
        .sm1 <- bigmemory::sub.big.matrix (.local$bm, firstRow=1, lastRow=nrow(.local$bm)-1)
        .sm2 <- bigmemory::sub.big.matrix (.local$bm, firstRow=2, lastRow=nrow(.local$bm))
        if (length(.cols) == 1) {
            .breaks <- which (.sm1[,.cols] != .sm2[,.cols])
        } else {
            .breaks <- which (!apply (.sm1[,.cols] == .sm2[,.cols], 1, all))
        }
        rm (.sm1, .sm2)
        .breaks <- .breaks + 1

        .local$last
    })

    # (2) work out if there's a group change between local[1] and local[2] etc.
    trans <- do.call (c, trans)
    tg <- test_transition (.self, .cols, trans)

    # (3) set breaks=1 for all where there's a transition
    .self$cluster_export_each ("tg", ".tg")
    .self$cluster_eval ({
        if (.local$empty) { return (NULL) }
        if (.tg) {
            .breaks <- c(1, .breaks)
        }
        NULL
    })

    # (4) filter at breaks
    .self$cluster_eval ({
        if (.local$empty) { return (NULL) }
        .local$filter_rows (.breaks)
        NULL
    })
    .self$filtered <- TRUE

    if (auto_compact) {
        .self$compact()
        .self$calc_group_sizes()
        return (.self)
    }

    .self$calc_group_sizes()

    # Repartition by group if appropriate
    if (regroup_partition) {
        return (.self %>% partition_group())
    } else {
        if (.self$grouped) {
            .self$rebuild_grouped()
        }
        return (.self)
    }

    return(.self)
}

#' @describeIn filter
#' @export
filter_ <- function (.self, ..., .dots, auto_compact = NULL) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }

    .self$cluster_export (c(".dots"))
    .self$cluster_eval ({
        if (.local$empty) { return (NULL) }
        if (.local$grouped) {
            for (.g in 1:length(.groups)) {
                for (.i in 1:length(.dots)) {
                    if (.grouped[[.g]]$empty) { next }
                    .res <- lazyeval::lazy_eval (.dots[.i], .grouped[[.g]]$envir())
                    .grouped[[.g]]$filter_vector (.res[[1]])
                }
            }
        } else {
            for (.i in 1:length(.dots)) {
                .res <- lazyeval::lazy_eval (.dots[.i], .local$envir())
                .local$filter_vector (.res[[1]])
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

#' @describeIn group_by
#' @export
group_by_ <- function (.self, ..., .dots, .cols=NULL, auto_partition=NULL) {
    if (is.null(.cols)) {
        .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
        namelist <- .dots2names (.dots)

        .cols <- match(namelist, .self$col.names)
    }
    if (is.null(auto_partition)) {
        auto_partition <- .self$auto_partition
    }
    N <- length(.self$cls)

    .self$sort (decreasing=FALSE, cols=.cols)

    .self$group.cols <- .cols

    if (nrow(.self$bm) == 1) {
        .self$bm[, .self$groupcol] <- 1
        .self$group_sizes <- 1
        .self$group_max <- 1
        return (.self)
    }

    if (N == 1) {
        if (nrow(.self$bm) == 2) {
            i <- ifelse (all(.self$bm[1, .cols] ==
                                 .self$bm[2, .cols]), 1, 2)
            .self$bm[, .self$groupcol] <- 1:i
            .self$group_sizes <- rep(1, i)
            .self$group_max <- i
            return (.self)
        }
        sm1 <- bigmemory::sub.big.matrix (.self$bm, firstRow=1, lastRow=nrow(.self$bm)-1)
        sm2 <- bigmemory::sub.big.matrix (.self$bm, firstRow=2, lastRow=nrow(.self$bm))
        if (length(.cols) == 1) {
            breaks <- which (sm1[,.cols] != sm2[,.cols])
        } else {
            breaks <- which (!apply (sm1[,.cols] == sm2[,.cols], 1, all))
        }
        sizes <- c(breaks, 100) - c(0, breaks)
        breaks <- c(0, breaks) + 1
        last <- 0
        for (i in 1:length(breaks)) {
            .self$bm[(last+1):breaks[i], .self$groupcol] <- i
            last <- breaks[i]
        }
        .self$group_sizes <- sizes
        .self$group_max <- length(sizes)
        return (.self)
    }

    # (0) If partitioned by group, temporarily repartition evenly
    regroup_partition <- .self$group_partition
    if (.self$group_partition) {
        .self$partition_even()
    }

    # (1) determine local groupings
    .self$cluster_export (c(".cols"))
    trans <- .self$cluster_eval ({
        if (.local$empty) { return (NA) }
        if (nrow(.local$bm) == 1) {
            .breaks <- 1
            return (1)
        } else if (nrow(.local$bm) == 2) {
            i <- ifelse (all(.local$bm[1, .cols] ==
                                 .local$bm[2, .cols]), 1, 2)
            .local$bm[, .local$groupcol] <- 1:i
            .breaks <- 1:i
            .local$group_sizes <- rep(1, i)
            .local$group_max <- i
            return (i)
        }
        .sm1 <- bigmemory::sub.big.matrix (.local$bm, firstRow=1, lastRow=nrow(.local$bm)-1)
        .sm2 <- bigmemory::sub.big.matrix (.local$bm, firstRow=2, lastRow=nrow(.local$bm))
        if (length(.cols) == 1) {
            .breaks <- which (.sm1[,.cols] != .sm2[,.cols])
        } else {
            .breaks <- which (!apply (.sm1[,.cols] == .sm2[,.cols], 1, all))
        }
        rm (.sm1, .sm2)

        .breaks <- c(.breaks, nrow(.local$bm))
        .prev <- 0
        for (.i in 1:length(.breaks)) {
            .local$bm[(.prev+1):.breaks[.i], .local$groupcol] <- .i
            .prev <- .breaks[.i]
        }

        .local$last
    })

    # (2) work out if there's a group change between local[1] and local[2] etc.
    trans <- do.call (c, trans)
    tg <- test_transition (.self, .cols, trans)
    tg <- tg[-1]

    # (3) add group base to each local
    Gcount <- do.call (c, .self$cluster_eval ({
        .local$bm[nrow(.local$bm), .local$groupcol]
    }))
    Gcount <- Gcount[-length(Gcount)]

    Gtr <- rep(1, length(Gcount))
    Gtr[tg] <- 0
    Gbase <- cumsum(c(0, Gcount-Gtr))
    .self$cluster_export_each ("Gbase", ".Gbase")
    .self$cluster_eval ({
        .local$bm[, .local$groupcol] <- .local$bm[, .local$groupcol] + .Gbase
        .groups <- unique (.local$bm[, .local$groupcol]) #FIXME
        NULL
    })

    # (4) figure out group sizes
    res <- .self$cluster_eval (.breaks)
    for (i in 1:length(res)) {
        if (length(res[[i]]) > 1) {
            for (j in 2:length(res[[i]])) {
                res[[i]][j] <- res[[i]][j] - res[[i]][j-1]
            }
        }
    }
    sizes <- res[[1]]
    for (i in 2:length(res)) {
        if (!tg[i-1]) {
            sizes[length(sizes)] <- sizes[length(sizes)] +
                res[[i]][1]
            sizes <- c(sizes, res[[i]][-1])
        } else {
            sizes <- c(sizes, res[[i]])
        }
    }

    .self$group_sizes <- sizes
    .self$group_max <- length(sizes)
    .self$group_sizes_stale <- FALSE

    .self$grouped <- TRUE

    # Input      Gcount   tg      Gbase  Output
    # 1: G=1,2   2        FALSE   0      G=1,2
    # 2: G=1,2   2        TRUE    1      G=2,3
    # --transition between 2->3--
    # 3: G=1,2                           G=4,5

    if (auto_partition && !regroup_partition) {
        .self$group_partition <- TRUE
        regroup_partition <- TRUE
    }

    # Repartition by group if appropriate
    if (regroup_partition) {
        return (.self %>% partition_group())
    } else {
        .self$rebuild_grouped()
        .self$update_fields ("grouped")
        return (.self)
    }
}

#' Return size of groups
#' @param .self Data frame
#' @export
group_sizes <- function (.self) {
    .self$calc_group_sizes (delay=FALSE)
    .self$group_sizes
}

#' @describeIn mutate
#' @export
mutate_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .resnames <- names(.dots)
    .rescols <- .self$alloc_col (.resnames, update=TRUE)

    .self$cluster_export (c(".resnames", ".rescols", ".dots"))
    .self$cluster_eval ({
        if (.local$empty) { return (NULL) }
        if (.local$grouped) {
            for (.g in 1:length(.groups)) {
                for (.i in 1:length(.dots)) {
                    .res <- lazyeval::lazy_eval (.dots[.i], .grouped[[.g]]$envir())
                    .grouped[[.g]]$set_data (, .rescols[.i], .res[[1]])
                }
            }
        } else {
            for (.i in 1:length(.dots)) {
                .res <- lazyeval::lazy_eval (.dots[.i], .local$envir())
                .local$set_data (, .rescols[.i], .res[[1]])
            }
        }
        NULL
    })
    if (any(.rescols == .self$group.cols)) {
        .self$calc_group_sizes()
    }
    return (.self)
}

#' Partition data evenly amongst cluster nodes
#' @param .self Data frame
#' @export
partition_even <- function (.self) {
    .self$partition_even ()
    .self$group_partition <- FALSE
    .self$update_fields ("group_partition")
    return(.self)
}

#' @describeIn partition_group
#' @export
partition_group_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)

    if (length(.dots) > 0) {
        .self$group_partition <- TRUE
        return (group_by_ (.self, .dots=.dots))
        #group_by_ calls partition_group() on its return
    }

    N <- length(.self$cls)

    G <- .self$group_sizes
    if (length(G) == 1) {
        Gi <- distribute (1, N)
        Gi[Gi == 0] <- NA
    } else {
        Gi <- distribute (G, N)
    }

    .self$cluster_export_each ("Gi", ".groups")

    .self$cluster_eval ({
        if (NA %in% .groups) {
            .local$empty <- TRUE
            return (NULL)
        }

        .local <- .master$copy(shallow=TRUE)
        .local$empty <- FALSE

        NULL
    })

    .self$rebuild_grouped ()
    .self$group_partition <- .self$grouped <- TRUE
    .self$update_fields (c("grouped", "group_partition"))

    return (.self)
}

#' Return to grouped data
#' @param .self Data frame
#' @export
regroup <- function (.self, auto_partition=NULL) {
    # This relies upon grouping column being unchanged

    if (is.null(auto_partition)) {
        auto_partition <- .self$auto_partition
    }
    .self$grouped <- TRUE
    .self$update_fields ("grouped")

    if (auto_partition) {
        .self$group_partition <- TRUE
        .self$update_fields ("group_partition")
    }

    return (.self)
}

#' @describeIn rename
#' @export
rename_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    newnames <- names(.dots)
    oldnames <- as.vector (vapply (.dots, function (x) { as.character (x$expr) }, ""))
    match <- match(oldnames, .self$col.names)
    .self$col.names[match] <- newnames
    .self$update_fields ("col.names")
    return (.self)
}

#' @describeIn select
#' @export
select_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    coln <- as.vector (vapply (.dots, function (x) { as.character (x$expr) }, ""))
    cols <- match (coln, .self$col.names)

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

#' @export
slice <- function (.self, rows=NULL, start=NULL, end=NULL, each=FALSE, auto_compact=NULL) {
    if (is.null(rows) && (is.null(start) || is.null(end))) {
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
                if (.local$empty) { return (NULL) }
                if (.self$grouped) {
                    for (.g in 1:length(.groups)) {
                        .grouped[[.g]]$filter_range (.start, .end)
                    }
                } else {
                    .local$filter_range (.start, .end)
                }
                NULL
            })
        } else if (is.logical(rows)) {
            .self$cluster_export ("rows", ".rows")
            .self$cluster_eval ({
                if (.local$empty) { return (NULL) }
                if (.self$grouped) {
                    for (.g in 1:length(.groups)) {
                        .grouped[[.g]]$filter_vector (.rows)
                    }
                } else {
                    .local$filter_vector (.rows)
                }
                NULL
            })
        } else {
            .self$cluster_export ("rows", ".rows")
            .self$cluster_eval ({
                if (.local$empty) { return (NULL) }
                if (.self$grouped) {
                    for (.g in 1:length(.groups)) {
                        .grouped[[.g]]$filter_rows (.rows)
                    }
                } else {
                    .local$filter_rows (.rows)
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

    .self$calc_group_sizes()

    if (auto_compact) {
        .self$compact()
    }

    .self
}

#' @describeIn summarise
#' @export
summarise_ <- function (.self, ..., .dots, auto_compact = NULL) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    if (is.null(auto_compact)) {
        auto_compact <- .self$auto_compact
    }
    avail <- which (substr (.self$col.names, 1, 1) != ".")
    if (.self$grouped) {
        avail <- avail[-.self$group.cols]
    }
    newnames <- names(.dots)
    if (length(newnames) > length(avail)) {
        stop ("Insufficient free columns available")
    }
    .newcols <- avail[1:length(newnames)]
    #FIXME: preserve type?

    .self$cluster_export (c(".dots", ".newcols"))
    .self$cluster_eval ({
        if (.local$empty) { return (NULL) }
        if (.local$grouped) {
            for (.g in 1:length(.grouped)) {
                .res <- lazyeval::lazy_eval (.dots, .grouped[[.g]]$envir())
                .len <- 0
                for (.i in 1:length(.res)) {
                    .grouped[[.g]]$bm[, .newcols[.i]] <- .res[[.i]]
                    if (length(.res[[.i]]) > .len) {
                        .len <- length(.res[[.i]])
                    }
                }
                .grouped[[.g]]$bm[, .grouped[[.g]]$filtercol] <- 0
                .grouped[[.g]]$bm[1:.len, .grouped[[.g]]$filtercol] <- 1
                .grouped[[.g]]$filtered <- TRUE
            }
        } else {
            .res <- lazyeval::lazy_eval (.dots, .local$envir())
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
        }
        NULL
    })
    .self$filtered <- TRUE
    if (auto_compact) {
        .self$compact()
    }

    .self$calc_group_sizes()

    .self$free_col (avail, update=TRUE)
    .self$alloc_col (newnames, update=TRUE)

    return (.self)
}

#' @describeIn transmute
#' @export
transmute_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)

    #mutate
    .resnames <- names(.dots)
    .rescols <- .self$alloc_col (.resnames, update=FALSE)

    .self$cluster_export (c(".resnames", ".rescols", ".dots"))
    .self$cluster_eval ({
        if (.local$empty) { return (NULL) }
        if (.local$grouped) {
            for (.g in 1:length(.groups)) {
                for (.i in 1:length(.dots)) {
                    .res <- lazyeval::lazy_eval (.dots[.i], .grouped[[.g]]$envir())
                    .grouped[[.g]]$set_data (, .rescols[.i], .res[[1]])
                }
            }
        } else {
            for (.i in 1:length(.dots)) {
                .res <- lazyeval::lazy_eval (.dots[.i], .local$envir())
                .local$set_data (, .rescols[.i], .res[[1]])
            }
        }
        NULL
    })
    if (any(.rescols == .self$group.cols)) {
        .self$calc_group_sizes()
    }
    #/mutate

    #FIXME: do on local/grouped
    dropcols <- .self$order.cols > 0
    dropcols[.rescols] <- FALSE
    if (.self$grouped) {
        dropcols[.self$group.cols] <- FALSE
    }
    dropcols <- which (dropcols)

    .self$free_col (dropcols, update=TRUE)

    return (.self)
}

#' @describeIn undefine
#' @export
undefine_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    dropnames <- .dots2names (.dots)
    dropcols <- match (dropnames, .self$col.names)
    .self$free_col (dropcols, update=TRUE)
    return (.self)
}

#' @describeIn undefine
#' @export
unselect_ <- undefine_

#' Return data to non-grouped
#' @param .self Data frame
#' @param .dots Workaround for non-standard evaluation
#' @export
ungroup <- function (.self) {
    .self$grouped <- .self$group_partition <- FALSE
    .self$update_fields (c("grouped", "group_partition"))
    .self$partition_even ()
    return (.self)
}

#' @describeIn ungroup
#' @export
rowwise <- ungroup

#' @describeIn regroup
#' @export
groupwise <- regroup

#' Execute code within a group
#' @param .self Data frame
#' @param expr Code to execute
#' @export
within_group <- function (.self, expr) {
    expr <- substitute(expr)
    .self$cluster_export ("expr", ".expr")
    .self$cluster_eval({
        if (.local$empty) { return(NULL) }
        for (.g in 1:length(.groups)) {
            eval (.expr, envir = .grouped[[.g]]$envir())
        }
        NULL
    })
    .self
}

#' Execute code within a node
#' @param .self Data frame
#' @param expr Code to execute
#' @export
within_node <- function (.self, expr) {
    expr <- substitute(expr)
    .self$cluster_export ("expr", ".expr")
    .self$cluster_eval({
        if (.local$empty) { return(NULL) }
        eval (.expr, envir = .local$envir())
        NULL
    })
    .self
}
