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
distinct_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .filtercol <- match(".filter", .self$col.names)

    .tmpcol <- .self$alloc_col()
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
        Gcol <- match (".group", .self$col.names)
        .cols <- c(Gcol, .cols)
    }

    if (nrow(.self$bm) == 1) {
        return (.self)
    }

    .self$sort (decreasing=FALSE, cols=.cols)
    if (N == 1) {
        if (nrow(.self$bm) == 2) {
            .self$bm[2, .filtercol] <- ifelse (
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

        .self$filter_rows (.tmpcol, .filtercol, breaks)

        .self$free_col (.tmpcol)
        return (.self)
    }

    .self$cluster_export (c(".cols", ".filtercol", ".tmpcol"))

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

        .last
    })

    # (2) work out if there's a group change between local[1] and local[2] etc.
    trans <- do.call (c, trans)
    trans <- trans[-length(trans)]   #last row is not actually a transition
    trans.i <- which (!is.na(trans)) #subsetting gets stroppy with NA
    trans <- trans[trans.i]

    sm1 <- bigmemory::sub.big.matrix (.self$bm, firstRow=1, lastRow=nrow(.self$bm)-1)
    sm2 <- bigmemory::sub.big.matrix (.self$bm, firstRow=2, lastRow=nrow(.self$bm))
    if (length(.cols) == 1 || length(trans) == 1) {
        tg.a <- all(sm1[trans, .cols] != sm2[trans, .cols])
    } else {
        tg.a <- !apply (sm1[trans, .cols] == sm2[trans, .cols], 1, all)
    }
    rm (sm1, sm2)

    tg <- rep(TRUE, N)
    tg[trans.i+1] <- tg.a

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
        .local$filter_rows (.tmpcol, .filtercol, .breaks)
        NULL
    })

    .self$free_col (.tmpcol)

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
filter_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)

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

    return (.self)
}

#' @describeIn group_by
#' @export
group_by_ <- function (.self, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    namelist <- .dots2names (.dots)

    .cols <- match(namelist, .self$col.names)
    .Gcol <- match(".group", .self$col.names)
    N <- length(.self$cls)

    .self$sort (decreasing=FALSE, dots=.dots)

    .self$grouped <- TRUE
    .self$update_fields ("grouped")

    .self$group.cols <- .cols

    if (nrow(.self$bm) == 1) {
        .self$bm[, .Gcol] <- 1
        .self$group_sizes <- 1
        .self$group_max <- 1
        return (.self)
    }

    if (N == 1) {
        if (nrow(.self$bm) == 2) {
            i <- ifelse (all(.self$bm[1, .cols] ==
                                 .self$bm[2, .cols]), 1, 2)
            .self$bm[, .Gcol] <- 1:i
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
            .self$bm[(last+1):breaks[i], .Gcol] <- i
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
    .self$cluster_export (c(".cols", ".Gcol"))
    trans <- .self$cluster_eval ({
        if (.local$empty) { return (NA) }
        if (nrow(.local$bm) == 1) {
            .breaks <- 1
            return (1)
        } else if (nrow(.local$bm) == 2) {
            i <- ifelse (all(.local$bm[1, .cols] ==
                                 .local$bm[2, .cols]), 1, 2)
            .local$bm[, .Gcol] <- 1:i
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
            .local$bm[(.prev+1):.breaks[.i],.Gcol] <- .i
            .prev <- .breaks[.i]
        }

        .last
    })

    # (2) work out if there's a group change between local[1] and local[2] etc.
    trans <- do.call (c, trans)
    trans <- trans[-length(trans)]

    sm1 <- bigmemory::sub.big.matrix (.self$bm, firstRow=1, lastRow=nrow(.self$bm)-1)
    sm2 <- bigmemory::sub.big.matrix (.self$bm, firstRow=2, lastRow=nrow(.self$bm))
    if (length(.cols) == 1) {
        tg <- sm1[trans, .cols] != sm2[trans, .cols]
    } else {
        if (length(trans) == 1) {
            tg <- !all (sm1[trans, .cols] == sm2[trans, .cols])
        } else {
            tg <- !apply (sm1[trans, .cols] == sm2[trans, .cols], 1, all)
        }
    }
    rm (sm1, sm2)

    # (3) add group base to each local
    Gcount <- do.call (c, .self$cluster_eval ({
        .local$bm[nrow(.local$bm), .Gcol]
    }))
    Gcount <- Gcount[-length(Gcount)]

    Gtr <- rep(1, length(Gcount))
    Gtr[tg] <- 0
    Gbase <- cumsum(c(0, Gcount-Gtr))
    .self$cluster_export_each ("Gbase", ".Gbase")
    .self$cluster_eval ({
        .local$bm[, .Gcol] <- .local$bm[, .Gcol] + .Gbase
        .groups <- unique (.local$bm[, .Gcol]) #FIXME
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

    # Input      Gcount   tg      Gbase  Output
    # 1: G=1,2   2        FALSE   0      G=1,2
    # 2: G=1,2   2        TRUE    1      G=2,3
    # --transition between 2->3--
    # 3: G=1,2                           G=4,5

    # Repartition by group if appropriate
    if (regroup_partition) {
        return (.self %>% partition_group())
    } else {
        .self$rebuild_grouped()
        return (.self)
    }
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
            .empty <- TRUE
            return (NULL)
        }

        .local <- .master$copy(shallow=TRUE)

        NULL
    })

    .self$rebuild_grouped ()
    .self$update_fields ("group_partition")

    return (.self)
}

#' Return size of groups
#' @param .data Data frame
#' @export
group_sizes <- function (.data) {
    attr(.data, "group_sizes")
}

#' @describeIn rename
#' @export
rename_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .newnames <- names(.dots)
    .oldnames <- as.vector (vapply (.dots, function (x) { as.character (x$expr) }, ""))
    .match <- match(.oldnames, attr(.data, "colnames"))
    attr(.data, "colnames")[.match] <- .newnames
    parallel::clusterExport (attr(.data, "cl"), c(".oldnames",
                                                  ".newnames",
                                                  ".match"),
                             envir=environment())
    parallel::clusterEvalQ (attr(.data, "cl"), {
        attr(.master, "colnames")[.match] <- .newnames
        attr(.local, "colnames")[.match] <- .newnames
        if (attr(.local, "grouped")) {
            for (.i in 1:length(.grouped)) {
                attr(.grouped[[.i]], "colnames")[.match] <- .newnames
            }
        }
        NULL
    })
    .data
}

#' @describeIn select
#' @export
select_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    coln <- as.vector (vapply (.dots, function (x) { as.character (x$expr) }, ""))
    cols <- match (coln, attr(.data, "colnames"))
    attr(.data, "order.cols") <- 0
    attr(.data, "order.cols")[sort(cols)] <- order(cols)
    # not propagated to .local on cluster as this is only for
    # display purposes (currently)
    .data
}

#' @export
slice <- function (.data, rows=NULL, start=NULL, end=NULL) {
    if (is.null(rows) && (is.null(start) || is.null(end))) {
        stop ("Must specify either rows or start and stop")
    } else if (!is.null(rows) && !(is.null(start) || is.null(end))) {
        stop ("Can either specify rows or start and stop; not both")
    }

    if (is.null(rows)) {
        rows <- start:end
    }

    #.filter_range?

    filtercol <- match(".filter", attr(.data, "colnames"))

    if (attr(.data, "grouped")) {
        if (attr(.data, "group_partition")) {
            .rows <- rows
            .filtercol <- filtercol
            parallel::clusterExport (attr(.data, "cl"), c(".rows",
                                                          ".filtercol"), envir=environment())
            parallel::clusterEvalQ (attr(.data, "cl"), {
                if (.empty) { return (NULL) }
                for (.g in 1:length(.groups)) {
                    .filtered <- bigmemory::mwhich (.grouped[[.g]][[1]],
                                                   cols=.filtercol,
                                                   vals=1,
                                                   comps="eq")
                    .rows <- .filtered[.rows]
                    .grouped[[.g]][[1]][, .filtercol] <- 0
                    .grouped[[.g]][[1]][.rows, .filtercol] <- 1
                }
                NULL
            })
        } else {
            #FIXME: pass g on to clusters to do parallel, i.e. temporary partition_group
            for (g in 1:attr(.data, "group_max")) {
                grouped <- group_restrict (.data, g)
                filtered <- bigmemory::mwhich (grouped[[1]],
                                               cols=filtercol,
                                               vals=1,
                                               comps="eq")
                rows <- filtered[rows]
                grouped[[1]][, filtercol] <- 0
                grouped[[1]][rows, filtercol] <- 1
            }
        }
    } else {
        #FIXME: partition rows across clusters or parition data frame across clusters?
        filtered <- bigmemory::mwhich (.data[[1]],
                                       cols=filtercol,
                                       vals=1,
                                       comps="eq")
        rows <- filtered[rows]
        .data[[1]][, filtercol] <- 0
        .data[[1]][rows, filtercol] <- 1
    }

    .data
}

#' @describeIn mutate
#' @export
mutate_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .resnames <- names(.dots)
    .rescols <- match (.resnames, attr(.data, "colnames"))
    needalloc <- which (is.na(.rescols))
    for (i in needalloc) {
        .data <- alloc_col (.data, .resnames[i])
        .rescols[i] <- match (.resnames[i], attr(.data, "colnames"))
    }

    cl <- attr (.data, "cl")
    parallel::clusterExport (cl, c(".resnames",
                                   ".rescols",
                                   ".dots"), envir=environment())
    parallel::clusterEvalQ (cl, {
        if (.empty) { return (NULL) }
        if (attr(.local, "grouped")) {
            for (.g in 1:length(.groups)) {
                for (.i in 1:length(.dots)) {
                    .res <- lazyeval::lazy_eval (.dots[.i], as.environment(no.strings.attached(.grouped[[.g]])))
                    .grouped[[.g]][[1]][, .rescols[.i]] <- factor_map (.grouped[[.g]], .rescols[.i], .res[[1]])
                }
            }
        } else {
            for (.i in 1:length(.dots)) {
                .res <- lazyeval::lazy_eval (.dots[.i], as.environment(no.strings.attached(.local)))
                .local[[1]][, .rescols[.i]] <- factor_map (.local, .rescols[.i], .res[[1]])
            }
        }
        NULL
    })
    .data
}

#' @describeIn transmute
#' @export
transmute_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    #mutate
    .resnames <- names(.dots)
    .rescols <- match (.resnames, attr(.data, "colnames"))
    needalloc <- which (is.na(.rescols))
    for (i in needalloc) {
        .data <- alloc_col (.data, .resnames[i])
        .rescols[i] <- match (.resnames[i], attr(.data, "colnames"))
    }

    cl <- attr (.data, "cl")
    parallel::clusterExport (cl, c(".resnames",
                                   ".rescols",
                                   ".dots"), envir=environment())
    parallel::clusterEvalQ (cl, {
        if (.empty) { return (NULL) }
        if (attr(.local, "grouped")) {
            for (.g in 1:length(.groups)) {
                for (.i in 1:length(.dots)) {
                    .res <- lazyeval::lazy_eval (.dots[.i], as.environment(no.strings.attached(.grouped[[.g]])))
                    .grouped[[.g]][[1]][, .rescols[.i]] <- factor_map (.grouped[[.g]], .rescols[.i], .res[[1]])
                    attr(.grouped[[.g]], "colnames")[.rescols[.i]] <- .resnames[.i]
                }
            }
        } else {
            for (.i in 1:length(.dots)) {
                .res <- lazyeval::lazy_eval (.dots[.i], as.environment(no.strings.attached(.local)))
                .local[[1]][, .rescols[.i]] <- factor_map (.local, .rescols[.i], .res[[1]])
                attr(.local, "colnames")[.rescols[.i]] <- .resnames[.i]
            }
        }
        NULL
    })
    #/mutate

    #FIXME: do on local/grouped
    dropcols <- setdiff (which(attr(.data, "order.cols") > 0), .rescols)
    .data <- free_col (.data, dropcols)

    attr(.data, "colnames")[.rescols] <- .resnames
    .data
}

#' @describeIn undefine
#' @export
undefine_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    nm <- names(.dots)
    for (i in 1:length(.dots)) {
        if (nm[i] == "") {
            var <- as.character(.dots[[i]]$expr)
        } else {
            var <- nm[i]
        }
        col <- match (var, attr(.data, "colnames"))
        .data <- free_col(.data, col)
    }
    .data
}

#' @describeIn undefine
#' @export
unselect_ <- undefine_

#' @export
compact <- function (.data, redistribute=FALSE) {
    cl <- attr(.data, "cl")

    .filtercol <- match (".filter", attr(.data, "colnames"))
    .redis <- redistribute
    parallel::clusterExport (cl, c(".filtercol",
                                   ".redis"), envir=environment())
    parallel::clusterEvalQ(cl, {
        if (.empty) { return (NULL) }
        bigmemory::mpermute (.local[[1]], cols=.filtercol, decreasing=TRUE)
        if (!.redis) {
            .complast <- match (0, .local[[1]][, .filtercol])
            if (!is.na (.complast)) {
                if (.complast == 1) {
                    .empty <- TRUE
                } else {
                    .local[[1]] <- sub.big.matrix(.local[[1]],
                                                  firstRow=1,
                                                  lastRow=.complast-1)
                }
            }
        }
        NULL
    })
    if (redistribute) {
        bigmemory::mpermute (.data[[1]], cols=.filtercol, decreasing=TRUE)
        complast <- match(0, .data[[1]][, .filtercol])
        if (!is.na (complast)) {
            if (complast == 1) {
                .empty <- TRUE
                parallel::clusterExport (cl, ".empty", envir=environment())
            } else {
                .data <- .data %>% partition (max.row = complast-1)
            }
        }
    }
    .data
}

#' Return to grouped data
#' @param .data Data frame
#' @export
regroup <- function (.data) {
    attr (.data, "grouped") <- TRUE
    parallel::clusterEvalQ (attr(.data, "cl"), attr(.local, "grouped") <- TRUE)
    return (.data)
}

#' Return data to non-grouped
#' @param .data Data frame
#' @param ... Additional parameters
#' @param .dots Workaround for non-standard evaluation
#' @export
ungroup <- function (.data) {
    attr (.data, "grouped") <- FALSE
    parallel::clusterEvalQ (attr(.data, "cl"), attr(.local, "grouped") <- FALSE)
    return (.data)
}

#' @describeIn ungroup
#' @export
rowwise <- ungroup

#' @describeIn regroup
#' @export
groupwise <- regroup

#' @describeIn summarise
#' @export
summarise_ <- function (.data, ..., .dots) {
    #FIXME: non-parallel & inefficient, but functional
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)

    if (!attr(.data, "grouped")) {
        res <- lazyeval::lazy_eval(.dots, as.environment(.data))

        # (1) select special columns
        keep <- which(substr (attr(.data, "colnames"), 1, 1) == ".")
        attr(.data, "colnames")[-keep] <- NA
        attr(.data, "order.cols")[-keep] <- 0

        # (2) alloc new columns
        rescols <- c()
        for (i in 1:length(res)) {
            var <- names(.dots)[i]
            if (var != "") {
                .data <- alloc_col (.data, var)
                rescols <- c(rescols, match(var, attr(.data, "colnames")))
            } else {
                stop ("FIXME")
            }
        }

        # (3) import
        for (i in 1:length(res)) {
            .data[[1]][, rescols[i]] <- res[[i]]
        }
        filtercol <- match (".filter", attr(.data, "colnames"))
        .filter_range (.data, filtercol, 1, length(res[[1]]))
    } else {
        for (g in 1:attr(.data, "group_max")) {
            grouped <- group_restrict (.data, g)
            res <- lazyeval::lazy_eval(.dots, as.environment(grouped))

            # (1) select special columns
            keep <- which(substr (attr(grouped, "colnames"), 1, 1) == ".")
            keep <- unique(c(keep, attr(grouped, "group_cols")))
            attr(grouped, "colnames")[-keep] <- NA
            attr(grouped, "order.cols")[-keep] <- 0

            # (2) alloc new columns
            rescols <- c()
            for (i in 1:length(res)) {
                var <- names(.dots)[i]
                if (var != "") {
                    grouped <- alloc_col (grouped, var)
                    rescols <- c(rescols, match(var, attr(grouped, "colnames")))
                } else {
                    stop ("FIXME")
                }
            }

            # (3) import
            for (i in 1:length(res)) {
                grouped[[1]][, rescols[i]] <- res[[i]]
            }
            filtercol <- match (".filter", attr(grouped, "colnames"))
            .filter_range (grouped, filtercol, 1, length(res[[1]]))
        }
        attr(.data, "colnames") <- attr(grouped, "colnames")
        attr(.data, "order.cols") <- attr(grouped, "order.cols")
    }
    .data
}

#' Execute code within a group
#' @param .data Data frame
#' @param .expr Code to execute
#' @export
within_group <- function (.data, .expr) {
    .expr <- substitute(.expr)
    parallel::clusterExport (attr(.data, "cl"), c(".expr"), envir = environment())
    parallel::clusterEvalQ (attr(.data, "cl"), {
        if (length(.groups) == 0) { return (NULL) }
        for (.g in 1:length(.groups)) {
            eval (.expr, envir = as.environment(.grouped[[.g]]))
        }
        NULL
    })
    .data
}

#' Execute code within a node
#' @param .data Data frame
#' @param .expr Code to execute
#' @export
within_node <- function (.data, .expr) {
    .expr <- substitute(.expr)
    parallel::clusterExport (attr(.data, "cl"), c(".expr"), envir = environment())
    parallel::clusterEvalQ (attr(.data, "cl"), {
        eval (.expr, envir = as.environment(.local))
        NULL
    })
    .data
}
