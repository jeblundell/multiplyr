# Misc functions that don't belong elsewhere

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

#' @export
pad.cols <- function (x, max.row=10) {
    #FIXME: more efficient way of doing this?
    if (is.null (max.row) || max.row==0) {
        max.row <- nrow(x[[1]])
    }
    pc <- attr(x, "pad")
    for (i in seq_len(ncol(x[[1]]))[pc==0]) {
        pc[i] <- max(nchar(as.character(x[[1]][1:max.row,i])))
    }
    pc
}

#' @export
bind_variables <- function (dat, envir) {
    rm (list=ls(envir=envir), envir=envir)
    for (var in names(dat)) {
        if (!attr(dat, "nsa")) {
            f <- local ({
                .var<-var
                .dat<-dat
                function (x) {
                    .dat[.var]
                }
            })
        } else {
            f <- local ({
                .var<-var
                .dat<-dat
                function (x) {
                    .col <- match(.var, attr(.dat, "colnames"))
                    .dat[[1]][, .col]
                }
            })
        }
        makeActiveBinding(var, f, env=envir)
    }
    return (ls(envir=envir))
}

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
