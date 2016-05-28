# Misc functions that don't belong elsewhere

#' Tests whether elements of a vector lie between two values (inclusively)
#'
#' @family utility functions
#' @param x Values to test
#' @param left Left value
#' @param right Right value
#' @return x >= left & x <= right
#' @export
between <- function (x, left, right) {
    x >= left & x <= right
}

#' Cumulative all
#'
#' @family utility functions
#' @param x Values
#' @return Cumulative all of x
#' @export
cumall <- function (x) {
    if (!x[[1]]) {
        return (rep(FALSE, length(x)))
    }

    m <- match (FALSE, x)
    if (is.na(m)) {
        return (rep(TRUE, length(x)))
    } else {
        return (c(rep(TRUE, m-1), rep(FALSE, (length(x)-m)+1)))
    }
}

#' Cumulative any
#'
#' @family utility functions
#' @param x Values
#' @return Cumulative any of x
#' @export
cumany <- function (x) {
    if (x[[1]]) {
        return (rep(TRUE, length(x)))
    }

    m <- match (TRUE, x)
    if (is.na(m)) {
        return (rep(FALSE, length(x)))
    } else {
        return (c(rep(FALSE, m-1), rep(TRUE, (length(x)-m)+1)))
    }
}

#' Cumulative mean
#'
#' @family utility functions
#' @param x Values to obtain cumulative mean of
#' @return Cumulative mean of x
#' @export
cummean <- function (x) {
    cumsum (x) / 1:length(x)
}

#' Calculations for how to distribute x items over N nodes
#'
#' This function is used to determine how to distribute the contents of a
#' data frame across the cluster. It may either be called with a single number
#' representing the total number of rows, or it may be called with a vector of
#' numbers representing the size of groups.
#'
#' @family utility functions
#' @param x Number of items or a vector of group sizes
#' @param N Number of nodes
#' @return A vector containing number of rows or a list containing the indices of groups
#' @export
#' @examples
#' distribute (100, 4)
#' distribute (c(25, 25, 50), 2)
distribute <- function (x, N) {
    if (length(x) == 1) {
        res <- rep(floor(x / N), N)
        rem <- (x - sum(res))       #left over from rounding
        i <- sample(1:N, rem)       #load balance
        res[i] <- res[i] + 1
        return (res)
    } else {
        bin.indices <- list()
        bin.size <- c()
        for (i in order(x, decreasing=TRUE)) {
            if (length(bin.size) < N) {
                bin.indices <- append (bin.indices, c(i))
                bin.size <- c(bin.size, x[i])
            } else {
                b <- which (bin.size == min(bin.size))
                if (length(b) > 1) {
                    b <- sample (b, 1)
                }
                bin.indices[[b]] <- c(bin.indices[[b]], i)
                bin.size[b] <- bin.size[b] + x[i]
            }
        }
        if (length(bin.indices) < N) {
            bin.indices <- append(bin.indices, NA)
        }
        return (bin.indices)
    }
}

#' Returns first value in vector
#'
#' @family utility functions
#' @param x Vector
#' @param default Value to pad missing data with
#' @return First value in x
#' @export
first <- function (x, default=NA) {
    if (length(x) == 0) {
        return (default)
    }
    x[[1]]
}

#' Offset x backwards by n
#'
#' @family utility functions
#' @param x Vector
#' @param n Amount of offset by
#' @param default Value to pad missing data with
#' @return Offset values
#' @export
lag <- function (x, n=1, default=NA) {
    if (length(x) <= n) {
        return (rep(default, n))
    }
    c(rep(default, n), x[1:(length(x)-n)])
}

#' Returns last value in vector
#'
#' @family utility functions
#' @param x Vector
#' @param default Value to pad missing data with
#' @return Last value in x
#' @export
last <- function (x, default=NA) {
    if (length(x) == 0) {
        return (default)
    }
    x[[length(x)]]
}

#' Offset x forwards by n
#'
#' @family utility functions
#' @param x Vector
#' @param n Amount of offset by
#' @param default Value to pad missing data with
#' @return Offset values
#' @export
lead <- function (x, n=1, default=NA) {
    if (length(x) <= n) {
        return (rep(default, n))
    }
    c(x[(n+1):length(x)],rep(default, n))
}

#' Number of items in current group
#'
#' @family utility functions
#' @return Number of items in current group (or node if ungrouped)
#' @export
n <- function () {
    if (exists(".local", inherits=TRUE)) {
        .local <- get(".local", inherits=TRUE)
        if (.local$filtered) {
            return (sum(.local$bm[, .local$filtercol]))
        } else {
            return (nrow(.local$bm))
        }
    } else {
        stop ("This function may only be used within mutate, transmute etc.")
    }
}

#' Return the number of unique values
#'
#' @family utility functions
#' @param x Vector
#' @return Number of unique values of x
#' @export
n_distinct <- function (x) {
    length(unique(x))
}

#' Return the nth item from a vector
#'
#' @family utility functions
#' @param x Vector
#' @param n The n in nth
#' @param default Value to pad missing data with
#' @return nth item from vector
#' @export
nth <- function (x, n, default=NA) {
    if (n > length(x)) {
        return (default)
    }
    x[[n]]
}

