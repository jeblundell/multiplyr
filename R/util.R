# Misc functions that don't belong elsewhere

#' Calculations for how to distribute x items over N nodes
#'
#' This function is used to determine how to distribute the contents of a
#' data frame across the cluster. It may either be called with a single number
#' representing the total number of rows, or it may be called with a vector of
#' numbers representing the size of groups.
#'
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
