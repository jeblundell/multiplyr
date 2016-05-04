# Internal functions not really intended for public use

#' Internal functions
#' @name internal
#' @keywords internal
NULL

#' @export
#' @keywords internal
#' @rdname internal
.p <- function (...) { paste (..., sep="") }

#' @export
#' @keywords internal
#' @rdname internal
.dots2names <- function (dots) {
    nm <- names (dots)
    exprs <- nm == ""
    if (any(exprs)) {
        nm[exprs] <- as.vector (sapply (dots[[exprs]], function (x) { as.character (x$expr) }))
    }
    return (nm)
}

#' @export
#' @keywords internal
#' @rdname internal
no.strings.attached <- function (x) {
    attr(x, "nsa") <- TRUE
    x
}

#' @export
#' @keywords internal
#' @rdname internal
.filter_range <- function (x, filtercol, start, end) {
    x[[1]][, filtercol] <- 0
    x[[1]][start:end, filtercol] <- 1
}

#' @export
#' @keywords internal
#' @export
test_transition <- function (.self, cols, rows) {
    N <- length(rows)
    rows.i <- which (!is.na(rows)) #subsetting gets stroppy with NA
    rows <- rows[rows.i]

    sm1 <- bigmemory::sub.big.matrix (.self$bm, firstRow=1, lastRow=nrow(.self$bm)-1)
    sm2 <- bigmemory::sub.big.matrix (.self$bm, firstRow=2, lastRow=nrow(.self$bm))
    if (length(cols) == 1 || length(rows) == 1) {
        tg.a <- any(sm1[rows, cols] != sm2[rows, cols])
    } else {
        tg.a <- !apply (sm1[rows, cols] == sm2[rows, cols], 1, all)
    }

    tg <- rep(TRUE, N)
    tg[rows.i] <- tg.a
    tg
}
