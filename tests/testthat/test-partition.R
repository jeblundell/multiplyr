context("partition")

#partition_even
#partition_group

cl2 <- parallel::makeCluster(2)

test_that("Multiplyr() partitions data evenly over 2 nodes by default", {
    dat <- Multiplyr(x=1:100,
                  f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                  G=rep(c("A", "B", "C", "D"), each=25),
                  alloc=1,
                  cl=cl2)
    res <- do.call (c, dat$cluster_eval(nrow(.local$bm)))
    expect_equal (res, c(50, 50))
    first <- do.call (c, dat$cluster_eval(.local$first))
    last <- do.call (c, dat$cluster_eval(.local$last))
    expect_equal (min(first), 1)
    expect_equal (min(last), 50)
    expect_equal (max(first), 51)
    expect_equal (max(last), 100)

    res <- sort (do.call (c, dat$cluster_eval(.local[, "x"])))
    expect_equal (res, 1:100)

    res <- sort (do.call (c, dat$cluster_eval(.local[, "f"])))
    expect_equal (res, rep(c(1, 2), each=50))

    res <- sort (do.call (c, dat$cluster_eval(.local[, "G"])))
    expect_equal (res, rep(c("A", "B", "C", "D"), each=25))

    rm(dat)
})

test_that("Multiplyr() partitions N=1 over 2 nodes sensibly", {
    dat <- Multiplyr (x=1, y=2, cl=cl2)
    res <- do.call (c, dat$cluster_eval(.local$empty))
    expect_equal (sort(res), c(FALSE, TRUE))
    res <- sort (do.call (c, dat$cluster_eval({
        if (.local$empty) { return (NA) }
        return (c(.local["x"], .local["y"]))
    })))
    expect_equal (res, c(1, 2))
    rm (dat)
})

test_that("Multiplyr() partitions N=2 over 2 nodes sensibly", {
    dat <- Multiplyr (x=1:2, y=1:2, cl=cl2)
    res <- do.call (c, dat$cluster_eval(nrow(.local$bm)))
    expect_equal (res, c(1, 1))
    first <- do.call (c, dat$cluster_eval(.local$first))
    last <- do.call (c, dat$cluster_eval(.local$last))
    expect_equal (sort(first), c(1,2))
    expect_equal (sort(last), c(1,2))
    expect_equal (first, last)
    rm (dat)
})

test_that("partition_group() can partition 2 groups over 2 nodes", {
    dat <- Multiplyr (x=1:100, G=rep(c(1,2), length.out=100), cl=cl2)
    dat %>% partition_group (G)
    res <- do.call (c, dat$cluster_eval(length(.local$group)))
    expect_equal (res, c(1, 1))
    res1 <- do.call (c, dat$cluster_eval({
        .local$group_restrict(.local$group[1])
        res <- sum(.local[, "G"]==1)
        .local$group_restrict()
        res
    }))
    res2 <- do.call (c, dat$cluster_eval({
        .local$group_restrict(.local$group[1])
        res <- sum(.local[, "G"]==2)
        .local$group_restrict()
        res
    }))
    expect_equal (sort(res1), c(0, 50))
    expect_equal (sort(res2), c(0, 50))
    expect_equal (res1, rev(res2))
    res <- dat$cluster_eval({
        .local$group_restrict (.local$group[1])
        res <- .local[, "x"]
        .local$group_restrict ()
        res
    })
    expect_equal (res[[1]], seq(1, 99, by=2))
    expect_equal (res[[2]], seq(2, 100, by=2))
    rm (dat)
})

test_that("partition_group() with 1 group uses only 1 node", {
    dat <- Multiplyr (x=1:100, G=rep(1, length.out=100), cl=cl2)
    dat %>% partition_group (G)
    res <- do.call (c, dat$cluster_eval (.local$empty))
    expect_equal (sort(res), c(FALSE, TRUE))
    rm (dat)
})

test_that("partition_group() with 3 groups partitions as 2,1 or 1,2", {
    dat <- Multiplyr (x=1:99, G=rep(c(1,2,3), length.out=99), cl=cl2)
    dat %>% partition_group (G)
    res <- do.call (c, dat$cluster_eval(length(.local$group)))
    expect_equal (sort(res), c(1,2))

    res <- dat$cluster_eval (.local$group)
    expect_equal (sort(do.call(c, res)), c(1, 2, 3))
    expect_equal (sort(c(length(res[[1]]),
                         length(res[[2]]))), c(1,2))
    rm (dat)
})

test_that("partition_group() with 2 levels of 2 groups partitions as 2,2 with 2 clusters", {
    dat <- Multiplyr (x=1:100, A=rep(c(1,2), each=50), B=rep(c(1,2), length.out=100), cl=cl2)
    dat %>% partition_group (A, B)
    res <- do.call (c, dat$cluster_eval(length(.local$group)))
    expect_equal (res, c(2,2))

    res <- dat$cluster_eval(.local$group)
    expect_equal (sort(do.call(c, res)), c(1, 2, 3, 4))

    rm (dat)
})

test_that ("partition_even() gives error if non-Multiplyr", {
    expect_error (data.frame (x=1:100) %>% partition_even(), "Multiplyr")
})

test_that ("partition_group() gives errors if no grouping or non-Multiplyr", {
    dat <- Multiplyr (x=1:100, A=rep(c(1,2), each=50), B=rep(c(1,2), length.out=100), cl=cl2)
    expect_error (dat %>% partition_group(), "group_by")
    expect_error (data.frame (x=1:100) %>% partition_group(x), "Multiplyr")
    rm (dat)
})

#Attempt to stop "no function to return from, jumping to top level"
gc()

parallel::stopCluster(cl2)
