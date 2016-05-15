context ("compact")

dat.df <- data.frame (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D"), each=25), stringsAsFactors=FALSE)
filts <- list(
    rep(c(TRUE, FALSE), 50),
    c(rep(c(TRUE, TRUE, FALSE), 33), TRUE),
    c(rep(TRUE, 50), rep(FALSE, 50)),
    rep(TRUE, 100),
    rep(FALSE, 100))

cl2 <- parallel::makeCluster (2)

test_that ("$filter_vector works appropriately", {
    for (i in 1:length(filts)) {
        f <- filts[[i]]
        info <- sprintf ("filter pattern %d", i)

        dat <- Multiplyr (dat.df, cl=cl2, auto_compact=FALSE)
        dat$filter_vector (f)

        expect_equal (dat$bm[, dat$filtercol], as.numeric(f), info=info)
        expect_true (dat$filtered, info=info)

        rm (dat)
    }
})

# $compact
test_that ("$compact() returns compacted data", {
    for (i in 1:length(filts)) {
        f <- filts[[i]]
        info <- sprintf ("filter pattern %d", i)

        dat <- Multiplyr (dat.df, cl=cl2, auto_compact=FALSE)
        dat$filter_vector (f)
        dat$compact ()

        expect_equal (dat["x"], dat.df$x[f], info=info)
        expect_equal (dat["y"], dat.df$y[f], info=info)
        expect_equal (dat["G"], dat.df$G[f], info=info)
        expect_false (dat$filtered, info=info)

        rm (dat)
    }
})

test_that ("$compact() does nothing to an empty data frame", {
    dat <- Multiplyr (dat.df, cl=cl2, auto_compact=FALSE)
    dat %>% filter (x<0)
    expect_silent (dat$compact())
    rm (dat)
})

test_that ("$compact() regroups grouped data", {
    for (i in 1:length(filts)) {
        f <- filts[[i]]
        info <- sprintf ("filter pattern %d", i)

        dat <- Multiplyr (dat.df, cl=cl2, auto_compact=FALSE)
        dat %>% group_by (G)
        Gcol <- match ("G", dat$col.names)

        dat$filter_vector (f)
        dat$compact ()

        expect_equal (dat["x"], dat.df$x[f], info=info)
        expect_equal (dat["y"], dat.df$y[f], info=info)
        expect_equal (dat["G"], dat.df$G[f], info=info)
        expect_false (dat$filtered, info=info)

        expect_true (dat$grouped, info=info)
        expect_true (dat$group_partition, info=info)
        expect_equal (dat$group.cols, Gcol, info=info)

        rm (dat)
    }
})

#FIXME: more thorough tests for non-compacted data
#test_that ("arrange() works on non-compacted data", { })
#test_that ("distinct() works on non-compacted data", { })
#test_that ("filter() works on non-compacted data", { })
#test_that ("group_by() works on non-compacted data", { })
#test_that ("group_sizes() works on non-compacted data", { })
#test_that ("mutate() works on non-compacted data", { })
#test_that ("partition_even() works on non-compacted data", { })
#test_that ("partition_group() works on non-compacted data", { })
#test_that ("regroup() works on non-compacted data", { })
#test_that ("rename() works on non-compacted data", { })
#test_that ("select() works on non-compacted data", { })
#test_that ("slice() works on non-compacted data", { })
#test_that ("summarise() works on non-compacted data", { })
#test_that ("transmute() works on non-compacted data", { })
#test_that ("undefine() works on non-compacted data", { })
#test_that ("ungroup() works on non-compacted data", { })
#test_that ("within_group() works on non-compacted data", { })
#test_that ("within_node() works on non-compacted data", { })

parallel::stopCluster (cl2)

