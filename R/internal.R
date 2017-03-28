# Internal functions not really intended for public use

#' Concatenate (internal)
#'
#' Shorthand for concatenating strings together
#'
#' @export
#' @keywords internal
#' @rdname p
#' @param ... Strings to be concatenated
#' @return Concetenated string
#' @examples
#' cat (.p("hello ", "world!"))
.p <- function (...) {
    paste0 (..., collapse="")
}

#' Extension of bigmemory::morder to allow decreasing parameter to be a vector
#'
#' @param x bigmemmory::big.matrix
#' @param cols Columns to sort on
#' @param na.last Handling of missing values: TRUE (last), FALSE (first), NA (omit)
#' @param decreasing Sort in decreasing order: single value or equal to length of x for tie breaking
#' @return Returns ordering vector
#' @export
#' @keywords internal
#' @examples
#' \donttest{
#' bm <- bigmemory::big.matrix (9, 3)
#' bm[] <- sample(1:3, 27, replace=TRUE)
#' bm[]
#' bm_morder (bm, cols=1, decreasing=c(TRUE, FALSE))
#' bm_morder (bm, cols=1, decreasing=c(FALSE, TRUE))
#' }
bm_morder <- function (x, cols, na.last=TRUE, decreasing=FALSE) {
    # Have left out checking whether just calling bigmemory::morder would be
    # more efficient here, so can do sensible unit testing. bm_mpermute will
    # check whether it's more efficient, though.
    if (length(decreasing) == 1) {
        decreasing <- rep(decreasing, length(cols))
    } else if (length(decreasing) < length(cols)) {
        decreasing <- c(decreasing, rep(decreasing[length(decreasing)], length(decreasing)-length(cols)))
    }
    if (decreasing[1]) {
        ord <- (nrow(x)+1) - rank(x[, cols[1]], ties.method="max", na.last = na.last)
    } else {
        ord <- rank(x[, cols[1]], ties.method="min", na.last = na.last)
    }
    ties <- nonunique (ord)
    i <- 2
    while (length(ties) > 0 && i <= length(cols)) {
        for (j in ties) {
            w <- ord == j
            if (decreasing[i]) {
                ordbrk <- (sum(w)+1) - rank(x[w, cols[i]], ties.method="max", na.last=na.last)
            } else {
                ordbrk <- rank(x[w, cols[i]], ties.method="min", na.last=na.last)
            }
            ord[w] <- (ord[w] - 1) + ordbrk
        }
        ties <- nonunique (ord)
        i <- i + 1
    }
    if (length(ties) > 0) {
        for (j in ties) {
            w <- ord == j
            ord[w] <- (ord[w] - 1) + 1:sum(w)
        }
    }
    #ord is actually a ranking, so turn it into an ordering vector
    #as.numeric needed due to bigmemory::mpermute throwing a strop
    return (as.numeric(order(ord)))
}

#' Extension of bigmemory::mpermute to allow decreasing parameter to be a vector
#'
#' @param x bigmemmory::big.matrix
#' @param order Ordering vector
#' @param cols Columns to sort on
#' @param allow.duplicates If TRUE allows row to be duplicated in result (order will be non-permutation of 1:nrow(x))
#' @param decreasing Sort in decreasing order: single value or equal to length of x for tie breaking
#' @param ... Additional parameters to pass to \code{\link{bm_morder}}
#' @return No return value: permutes in place
#' @export
#' @keywords internal
#' @examples
#' \donttest{
#' bm <- bigmemory::big.matrix (9, 3)
#' bm[] <- sample(1:3, 27, replace=TRUE)
#' bm[]
#' bm_mpermute (bm, cols=1, decreasing=c(TRUE, FALSE))
#' bm_mpermute (bm, cols=1, decreasing=c(FALSE, TRUE))
#' }
bm_mpermute <- function (x, order=NULL, cols=NULL, allow.duplicates=FALSE, decreasing=FALSE, ...) {
    if (all(decreasing) || all(!decreasing) || !is.null(order)) {
        decreasing <- decreasing[1]
    }
    if (length(decreasing) == 1) {
        bigmemory::mpermute (x, order=order, cols=cols, allow.duplicates=allow.duplicates, decreasing=decreasing, ...)
    } else {
        bigmemory::mpermute (x, order=bm_morder(x, cols=cols, decreasing=decreasing, ...), allow.duplicates=allow.duplicates)
    }
}

#' Capture ... for later evaluation
#'
#' @param ... Dots from function
#' @return Dots object for use with \code{\link{dotseval}}
#' @keywords internal
#' @export
#' @examples
#' \donttest{
#' f <- function (...) { dotscapture(...) }
#' x <- 123
#' dots <- f (x)
#' dotseval (dots, sys.frame())
#' }
dotscapture <- function (...) {
    dotsname (eval(substitute(alist(...))))
}

#' Combine explicit and implicit dots
#'
#' @return Dots object for use with \code{\link{dotseval}}
#' @keywords internal
#' @export
dotscombine <- function (dots, ...) {
    newdots <- dotscapture (...)
    if (missing(dots)) {
        return (newdots)
    }
    append(dots, newdots)
}

#' Evaluate previously captured dots
#'
#' @param dots Captured ... from \code{\link{dotscapture}}
#' @param env Environment to evaluate in
#' @return Results of evaluating expression
#' @keywords internal
#' @export
#' @examples
#' \donttest{
#' f <- function (...) { dotscapture(...) }
#' x <- 123
#' dots <- f (x)
#' dotseval (dots, sys.frame())
#' }
dotseval <- function (expr, envir) {
    lapply (expr, eval, env=envir, enclos=envir)
}

#' Ensure captured dots are all named
#'
#' @param dots Captured dots
#' @return Captured dots, all named
#' @export
#' @keywords internal
dotsname <- function (dots) {
    nm <- names(dots)
    if (is.null(nm)) {
        nm <- rep("", length(dots))
    }
    neednames <- which (nm == "")
    nm[neednames] <- vapply (dots[neednames], dotsname1, character(1), USE.NAMES=FALSE)
    names(dots) <- nm
    dots
}

#' Name an expression (called by dotsname)
#'
#' @param expr Expression
#' @return Name of expression
#' @export
#' @keywords internal
dotsname1 <- function (expr) {
    if (is.symbol(expr)) {
        return (as.character(expr))
    }
    return (deparse (expr))
}


#' Returns NA of a particular class
#'
#' This is a horrible kludge needed so copy() doesn't complain about
#' assigning NULL to a field when there's no cluster, i.e. for the slave nodes
#'
#' @name NA_class_
#' @param type Name of class
#' @return NA with class set to type
#' @keywords internal
#' @export
#' @examples
#' \donttest{
#' nacls <- NA_class_ ("SOCKcluster")
#' }
NA_class_ <- function (type) {
    res <- NA
    class(res) <- type
    return (res)
}

#' Returns values of x that are non-unique
#'
#' @export
#' @keywords internal
#' @examples
#' \donttest{
#' nonunique (c(1, 1, 1, 2, 3))
#' nonunique (c(1, 2, 3, 3, 4, 4))
#' }
nonunique <- function (x) {
    unique(x[duplicated(x)])
}

#' Returns big.matrix descriptor offset by 1 (for row by row comparisons)
#'
#' @param .self Data frame
#' @param start 1 or 2
#' @return big.matrix.descriptor subset to 1:last-1 or 2:last
#' @export
#' @keywords internal
sm_desc_comp <- function (.self, start) {
    if (start == 1) {
        return (sm_desc_update (.self$desc.master, .self$first, .self$last-1))
    } else if (start == 2) {
        return (sm_desc_update (.self$desc.master, .self$first+1, .self$last))
    }
}

#' Returns a big.matrix descriptor for a particular group ID
#'
#' @param .self Data frame
#' @param group_id Group ID (from groupcol)
#' @return big.matrix.descriptor that would restrict to this group
#' @export
#' @keywords internal
sm_desc_group <- function (.self, group_id) {
    return (sm_desc_update(.self$desc.master,
                           .self$group_cache[group_id, 1],
                           .self$group_cache[group_id, 2]))
}

#' Returns big.matrix descriptor limited to particular start/end row
#'
#' @param .self Data frame
#' @param first First row
#' @param last Last row
#' @return big.matrix.descriptor that would restrict to this group
#' @export
#' @keywords internal
sm_desc_subset <- function (.self, first, last) {
    return (sm_desc_update(.self$desc.master, first, last))
}

#' Update description of a big.matrix after a row subset (internal)
#'
#' Generating a new big.matrix.descriptor or doing sub.big.matrix on something
#' that's not a descriptor is slow. This method exists to effectively create
#' the descriptor that describe(new.sub.big.matrix) would do, but in a fraction
#' of the time.
#'
#' @param desc Existing big.matrix.descriptor
#' @param first First relative row of that matrix
#' @param last Last relative row of that matrix
#' @return New descriptor
#' @export
#' @keywords internal
sm_desc_update <- function (desc, first, last) {
    desc@description$rowOffset <- c(
        (desc@description$rowOffset[1] + first) - 1,
        (last-first)+1)
    desc@description$nrow <- (last - first)+1
    return (desc)
}

#' Test for grouping transition (internal)
#'
#' This algorithm tests specific rows for a transition. Each cluster node will
#' test its own subset for grouping transitions, but the transition between the
#' last row of one cluster and the first of the next needs to be tested on the
#' master node.
#'
#' @param .self Parallel data frame
#' @param cols Columns to test
#' @param rows Rows to test
#' @return Logical vector of which rows have a transition (first element is always TRUE)
#' @export
#' @keywords internal
test_transition <- function (.self, cols, rows) {
    N <- length(rows)
    rows.i <- which (!is.na(rows)) #subsetting gets stroppy with NA
    rows <- rows[rows.i]

    sm1 <- bigmemory.sri::attach.resource(sm_desc_comp (.self, 1))
    sm2 <- bigmemory.sri::attach.resource(sm_desc_comp (.self, 2))
    if (length(cols) == 1 || length(rows) == 1) {
        tg.a <- any(sm1[rows, cols] != sm2[rows, cols])
    } else {
        tg.a <- !apply (sm1[rows, cols] == sm2[rows, cols], 1, all)
    }

    tg <- rep(TRUE, N)
    tg[rows.i] <- tg.a
    tg
}
