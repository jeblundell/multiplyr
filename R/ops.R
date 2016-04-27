# Operations on fastdf objects

#' Partition data evenly amongst cluster nodes
#' @param .data Data frame
#' @param max.row Partition only these number of rows. NULL (default) or 0
#'                will partition the entire data set
#' @export
partition <- function (.data, max.row = NULL) {
    if (is.null (max.row) || max.row == 0) {
        max.row <- nrow(.data[[1]])
    }

    .data <- .partition_all (.data, max.row = max.row)

    attr (.data, "group_partition") <- FALSE
    parallel::clusterEvalQ (attr(.data, "cl"), {
        attr(.local, "group_partition") <- FALSE
        attr(.master, "group_partition") <- FALSE
    })
    return(.data)
}

#' @describeIn partition_group
#' @export
partition_group_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)

    if (length(.dots) > 0) {
        attr (.data, "group_partition") <- TRUE
        return (group_by_ (.data, .dots=.dots))
        #group_by_ calls partition_group() on its return
    }

    cl <- attr(.data, "cl")
    N <- length(cl)

    G <- attr(.data, "group_sizes")
    if (length(G) == 1) {
        Gi <- distribute (1, N)
        Gi[Gi == 0] <- NA
    } else {
        Gi <- distribute (G, N)
    }

    for (i in 1:N) {
        .groups <- Gi[[i]]
        parallel::clusterExport (cl[i], ".groups", envir=environment())
    }

    parallel::clusterEvalQ (cl, {
        if (NA %in% .groups) {
            .empty <- TRUE
            return (NULL)
        }

        .local[[1]] <- .master[[1]]
        attr(.local, "group_partition") <- TRUE
        NULL
    })

    return (.rebuild_grouped (.data))
}

#' @export
cldo <- function (.data, ...) {
    parallel::clusterEvalQ (attr(.data, "cl"), ...)
}

#' @export
clget_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    parallel::clusterExport (attr(.data, "cl"), ".dots", envir=environment())
    parallel::clusterEvalQ (attr(.data, "cl"), {
        if (.empty) { return (NULL) }
        lazy_eval (.dots, as.environment(.local))
    })
}

#' @describeIn arrange
#' @export
arrange_ <- function (.data, ..., .dots) {
    #This works on the presumption that factors have levels
    #sorted already
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .sort.fastdf(.data, decreasing=FALSE, .dots, with.group = attr(.data, "grouped"))
}

#' @describeIn fast_filter
#' @export
fast_filter_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    parallel::clusterExport (attr(.data, "cl"), ".dots", envir=environment())

    parallel::clusterEvalQ (attr(.data, "cl"), {
        if (.empty) { return (NULL) }
        filtercol <- match (".filter", attr(.local, "colnames"))
        .local <- alloc_col (.local)
        .tmpcol <- match (".tmp", attr(.local, "colnames"))
        res <- ff_mwhich(.local, .dots)

        if (length(res) == 0) {
            .local[[1]][, filtercol] <- 0
        } else {
            .local[[1]][, .tmpcol] <- 0
            .local[[1]][res, .tmpcol] <- 1

            .local[[1]][, filtercol] <- .local[[1]][, filtercol] *
                .local[[1]][, .tmpcol]
        }
        .local <- free_col (.local, .tmpcol)
        NULL
    })
    return (.data)
}

#' @describeIn group_by
#' @export
group_by_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    namelist <- .dots2names (.data, .dots)
    .cols <- match(namelist, attr(.data, "colnames"))
    .Gcol <- match(".group", attr(.data, "colnames"))
    N <- length(attr(.data, "cl"))

    .sort.fastdf (.data, decreasing=FALSE, .dots)

    attr (.data, "grouped") <- TRUE
    parallel::clusterEvalQ (attr(.data, "cl"), attr(.local, "grouped") <- TRUE)

    attr (.data, "group_cols") <- .cols

    if (nrow(.data[[1]]) == 1) {
        .data[[1]][, .Gcol] <- 1
        attr(.data, "group_sizes") <- 1
        attr(.data, "group_max") <- 1
        return (.data)
    }

    if (N == 1) {
        if (nrow(.data[[1]]) == 2) {
            i <- ifelse (all(.data[[1]][1, .cols] ==
                                 .data[[1]][2, .cols]), 1, 2)
            .data[[1]][, .Gcol] <- 1:i
            attr(.data, "group_sizes") <- rep(1, i)
            attr(.data, "group_max") <- i
            return (.data)
        }
        sm1 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=1, lastRow=nrow(.data[[1]])-1)
        sm2 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=2, lastRow=nrow(.data[[1]]))
        if (length(.cols) == 1) {
            breaks <- which (sm1[,.cols] != sm2[,.cols])
        } else {
            breaks <- which (!apply (sm1[,.cols] == sm2[,.cols], 1, all))
        }
        sizes <- c(breaks, 100) - c(0, breaks)
        breaks <- c(0, breaks) + 1
        last <- 0
        for (i in 1:length(breaks)) {
            .data[[1]][(last+1):breaks[i],.Gcol] <- i
            last <- breaks[i]
        }
        attr (.data, "group_sizes") <- sizes
        attr (.data, "group_max") <- length(sizes)
        return (.data)
    }

    # (0) If partitioned by group, temporarily repartition evenly
    if (attr(.data, "group_partition")) {
        .data <- .partition_all (.data)
    }

    # (1) determine local groupings
    parallel::clusterExport (attr(.data, "cl"), c(".cols", ".Gcol"), envir=environment())
    trans <- parallel::clusterEvalQ (attr(.data, "cl"), {
        if (nrow(.local[[1]]) == 1) {
            .breaks <- 1
            return (1)
        } else if (nrow(.local[[1]]) == 2) {
            i <- ifelse (all(.local[[1]][1, .cols] ==
                                 .local[[1]][2, .cols]), 1, 2)
            .local[[1]][, .Gcol] <- 1:i
            .breaks <- 1:i
            attr(.local, "group_sizes") <- rep(1, i)
            attr(.local, "group_max") <- i
            return (i)
        }
        .sm1 <- bigmemory::sub.big.matrix (.local[[1]], firstRow=1, lastRow=nrow(.local[[1]])-1)
        .sm2 <- bigmemory::sub.big.matrix (.local[[1]], firstRow=2, lastRow=nrow(.local[[1]]))
        if (length(.cols) == 1) {
            .breaks <- which (.sm1[,.cols] != .sm2[,.cols])
        } else {
            .breaks <- which (!apply (.sm1[,.cols] == .sm2[,.cols], 1, all))
        }
        rm (.sm1, .sm2)

        .breaks <- c(.breaks, nrow(.local[[1]]))
        .prev <- 0
        for (.i in 1:length(.breaks)) {
            .local[[1]][(.prev+1):.breaks[.i],.Gcol] <- .i
            .prev <- .breaks[.i]
        }

        .last
    })

    # (2) work out if there's a group change between local[1] and local[2] etc.
    trans <- do.call (c, trans)
    trans <- trans[-length(trans)]

    sm1 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=1, lastRow=nrow(.data[[1]])-1)
    sm2 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=2, lastRow=nrow(.data[[1]]))
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
    Gcount <- do.call (c, parallel::clusterEvalQ (attr(.data, "cl"), .local[[1]][nrow(.local[[1]]), .Gcol]))
    Gcount <- Gcount[-length(Gcount)]

    Gtr <- rep(1, length(Gcount))
    Gtr[tg] <- 0
    Gbase <- cumsum(c(0, Gcount-Gtr))
    for (i in 1:N) {
        .Gbase <- Gbase[i]
        parallel::clusterExport(attr(.data, "cl")[i], ".Gbase", envir=environment())
    }
    parallel::clusterEvalQ (attr(.data, "cl"), {
        .local[[1]][, .Gcol] <- .local[[1]][, .Gcol] + .Gbase
        .groups <- unique (.local[[1]][, .Gcol]) #FIXME
        NULL
    })

    # (4) figure out group sizes
    res <- parallel::clusterEvalQ(attr(.data, "cl"), .breaks)
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

    attr(.data, "group_sizes") <- sizes
    attr(.data, "group_max") <- length(sizes)

    # Input      Gcount   tg      Gbase  Output
    # 1: G=1,2   2        FALSE   0      G=1,2
    # 2: G=1,2   2        TRUE    1      G=2,3
    # --transition between 2->3--
    # 3: G=1,2                           G=4,5

    # Repartition by group if appropriate
    if (attr(.data, "group_partition")) {
        return (.data %>% partition_group())
    } else {
        return (.rebuild_grouped (.data))
    }
}

#' Return size of groups
#' @param .data Data frame
#' @export
group_sizes <- function (.data) {
    attr(.data, "group_sizes")
}

#' @describeIn distinct
#' @export
distinct_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .filtercol <- match(".filter", attr(.data, "colnames"))
    .data <- alloc_col (.data)
    .tmpcol <- match (".tmp", attr(.data, "colnames"))
    N <- length (attr(.data, "cl"))

    if (length(.dots) > 0) {
        namelist <- .dots2names (.data, .dots)
        .cols <- match(namelist, attr(.data, "colnames"))
        .sort.fastdf (.data, decreasing=FALSE, dots=.dots)
    } else {
        .cols <- attr(.data, "order.cols") > 0
        .cols <- (1:length(.cols))[.cols]
        .sort.fastdf (.data, decreasing=FALSE, cols=.cols)
    }

    if (attr(.data, "grouped")) {
        Gcol <- match (".group", attr(.data, "colnames"))
        .cols <- c(Gcol, .cols)
    }

    if (nrow(.data[[1]]) == 1) {
        return (.data)
    }

    .sort.fastdf (.data, decreasing=FALSE, cols=.cols)
    if (N == 1) {
        if (nrow(.data[[1]]) == 2) {
            .data[[1]][2, .filtercol] <- ifelse (
                all(.data[[1]][1, .cols] == .data[[1]][2, .cols]), 0, 1)
            return (.data)
        }
        sm1 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=1, lastRow=nrow(.data[[1]])-1)
        sm2 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=2, lastRow=nrow(.data[[1]]))
        if (length(.cols) == 1) {
            breaks <- which (sm1[,.cols] != sm2[,.cols])
        } else {
            breaks <- which (!apply (sm1[,.cols] == sm2[,.cols], 1, all))
        }
        breaks <- c(0, breaks) + 1

        .data[[1]][, .tmpcol] <- 0
        .data[[1]][breaks, .tmpcol] <- 1
        .data[[1]][, .filtercol] <- .data[[1]][, .filtercol] *
            .data[[1]][, .tmpcol]

        .data <- free_col (.data, .tmpcol)
        return (.data)
    }

    parallel::clusterExport (attr(.data, "cl"), c(".cols",
                                                  ".filtercol",
                                                  ".tmpcol"),
                             envir=environment())


    # (0) If partitioned by group, temporarily repartition evenly
    if (attr(.data, "group_partition")) {
        .data <- .partition_all (.data)
    }

    # (1) determine local distinct rows
    trans <- parallel::clusterEvalQ (attr(.data, "cl"), {
        if (nrow(.local[[1]]) == 1) {
            .breaks <- 1
            return (1)
        } else if (nrow(.local[[1]]) == 2) {
            i <- ifelse (all(.local[[1]][1, .cols] ==
                                 .local[[1]][2, .cols]), 1, 2)
            .breaks <- 1:i
            return (i)
        }
        .sm1 <- bigmemory::sub.big.matrix (.local[[1]], firstRow=1, lastRow=nrow(.local[[1]])-1)
        .sm2 <- bigmemory::sub.big.matrix (.local[[1]], firstRow=2, lastRow=nrow(.local[[1]]))
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
    trans <- trans[-length(trans)]

    sm1 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=1, lastRow=nrow(.data[[1]])-1)
    sm2 <- bigmemory::sub.big.matrix (.data[[1]], firstRow=2, lastRow=nrow(.data[[1]]))
    if (length(.cols) == 1 || length(trans) == 1) {
        tg <- all(sm1[trans, .cols] != sm2[trans, .cols])
    } else {
        tg <- !apply (sm1[trans, .cols] == sm2[trans, .cols], 1, all)
    }
    rm (sm1, sm2)

    # (3) set breaks=1 for all where there's a transition
    tg <- c(TRUE, tg)
    for (i in 1:N) {
        if (tg[i]) {
            parallel::clusterEvalQ (attr(.data, "cl")[i], {
                .breaks <- c(1, .breaks)
                NULL
            })
        }
    }

    # (4) filter at breaks
    parallel::clusterEvalQ (attr(.data, "cl"), {
        .local[[1]][, .tmpcol] <- 0
        .local[[1]][.breaks, .tmpcol] <- 1
        .local[[1]][, .filtercol] <- .local[[1]][, .filtercol] *
            .local[[1]][, .tmpcol]
        NULL
    })

    .data <- free_col (.data, .tmpcol)

    # Repartition by group if appropriate
    if (attr(.data, "group_partition")) {
        return (.data %>% partition_group())
    }

    return(.data)
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

#' @describeIn filter
#' @export
filter_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    .filtercol <- match(".filter", attr(.data, "colnames"))

    cl <- attr (.data, "cl")
    parallel::clusterExport (cl, c(".filtercol",
                                   ".dots"), envir=environment())
    parallel::clusterEvalQ (cl, {
        if (.empty) { return (NULL) }
        if (attr(.local, "grouped")) {
            for (.g in 1:length(.groups)) {
                .res <- lazyeval::lazy_eval (.dots, as.environment(.grouped[[.g]]))
                for (.r in .res) {
                    .grouped[[.g]][[1]][, .filtercol] <- .grouped[[.g]][[1]][, .filtercol] * .r
                }
            }
        } else {
            .res <- lazyeval::lazy_eval (.dots, as.environment(.local))
            for (.r in .res) {
                .local[[1]][, .filtercol] <- .local[[1]][, .filtercol] * .r
            }
        }
        NULL
    })

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

#' @describeIn define
#' @export
define_ <- function (.data, ..., .dots) {
    .dots <- lazyeval::all_dots (.dots, ..., all_named=TRUE)
    nm <- names(.dots)
    for (i in 1:length(.dots)) {
        if (nm[i] == "") {
            var <- as.character(.dots[[i]]$expr)
            .data <- alloc_col (.data, var)
        } else {
            var <- nm[i]
            .data <- alloc_col (.data, nm[i])
            col <- match (var, attr(.data, "colnames"))

            tpl <- as.character(.dots[[i]]$expr)
            tcol <- match (tpl, attr(.data, "colnames"))
            attr(.data, "type.cols")[col] <- attr(.data, "type.cols")[tcol]
            if (attr(.data, "type.cols")[tcol] > 0) {
                f <- match (tcol, attr(.data, "factor.cols"))
                attr(.data, "factor.cols") <- c(attr(.data, "factor.cols"), col)
                attr(.data, "factor.levels") <- append(attr(.data, "factor.levels"), list(
                                                       attr(.data, "factor.levels")[[f]]))
            }
        }
    }
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
        .data[[1]][, filtercol] <- 0
        .data[[1]][1:length(res[[1]]), filtercol] <- 1
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
            grouped[[1]][, filtercol] <- 0
            grouped[[1]][1:length(res[[1]]), filtercol] <- 1
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
