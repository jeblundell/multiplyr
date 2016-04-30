# Misc functions that don't belong elsewhere

#' Calculations for how to distribute x items over N nodes
#' @param x Number of items or a vector of group sizes
#' @param N Number of nodes
#' @export
distribute <- function (x, N) {
    if (length(x) == 1) {
        res <- rep(floor(x / N), N)
        rem <- (x - sum(res))       #left over from rounding
        i <- sample(1:N, rem)       #load balance
        res[i] <- res[i] + 1
        return (res)
    } else {
        #FIXME: naive bin packing algorithm
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

#' Return a parallel data frame mapped to a particular group
#' @param dat Data frame
#' @param group Group ID
#' @export
group_restrict <- function (dat, group) {
    if (group <= 0) { return (dat) }
    attr(dat, "group") <- group

    #presumes that dat is sorted by grouping column first
    Gcol <- match (".group", attr(dat, "colnames"))
    lims <- range(which (dat[[1]][, Gcol] == attr(dat, "group")))
    dat[[1]] <- bigmemory::sub.big.matrix(dat[[1]],
                                          firstRow=lims[1],
                                          lastRow=lims[2])

    return (dat)
}

#' Display a warning if a condition is sub-optimal
#' @param expr Expression
#' @param warn Warning text
#' @export
warn.suboptimal <- function (expr, warn) {
    if (getOption("warn.suboptimal", default=FALSE)) {
        if (expr) {
            warning (warn, call.=FALSE)
        }
    }
}
