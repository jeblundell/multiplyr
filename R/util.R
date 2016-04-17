
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
