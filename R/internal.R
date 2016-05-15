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

#' Extract names from a lazy_dots object (internal)
#'
#' This will take a lazy_dots object and will extract all the names from it
#'
#' @param dots Lazy dots object
#' @return Vector of names
#' @export
#' @keywords internal
#' @rdname dots2names
#' @examples
#' f <- function (...) { lazyeval::lazy_dots (...) }
#' dots <- f(x, y=z)
#' .dots2names (dots)
.dots2names <- function (dots) {
    nm <- names (dots)
    exprs <- nm == ""
    if (any(exprs)) {
        nm[exprs] <- as.vector (sapply (dots[exprs], function (x) { as.character (x$expr) }))
    }
    return (nm)
}

#' Returns NA as a cluster object
#'
#' This is a horrible kludge needed so copy() doesn't complain about
#' assigning NULL to a field when there's no cluster, i.e. for the slave nodes
#'
#' @name clsna
#' @return NA with class set to SOCKcluster
#' @keywords internal
#' @export
.clsna <- function () {
    clsna <- NA
    class(clsna) <- "SOCKcluster"
    return (clsna)
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

    sm1 <- bigmemory::sub.big.matrix (.self$desc, firstRow=1, lastRow=nrow(.self$bm)-1)
    sm2 <- bigmemory::sub.big.matrix (.self$desc, firstRow=2, lastRow=nrow(.self$bm))
    if (length(cols) == 1 || length(rows) == 1) {
        tg.a <- any(sm1[rows, cols] != sm2[rows, cols])
    } else {
        tg.a <- !apply (sm1[rows, cols] == sm2[rows, cols], 1, all)
    }

    tg <- rep(TRUE, N)
    tg[rows.i] <- tg.a
    tg
}
