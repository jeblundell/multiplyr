context("groups")

test_that ("distinct() returns unique rows", {
    for (i in list(1:100, rep(1:50, length.out=100),
                   c(rep(1:25, each=2), rep(1:25, length.out=50)))) {
        dat <- fastdf (x=i, cl=2)
        dat <- dat %>% distinct (x)
        expect_equal (sort(dat$x), sort(unique(i)))
        stopCluster (attr(dat, "cl"))
        rm (dat)
    }
})

test_that ("distinct() works when N=1", {
    dat <- fastdf (x=1, y=1, cl=1)
    dat <- dat %>% distinct (x, y)
    expect_equal (dat$x, 1)
    expect_equal (dat$y, 1)
    stopCluster (attr(dat, "cl"))
    rm (dat)

    dat <- fastdf (x=1, y=1, cl=2)
    dat <- dat %>% distinct (x, y)
    expect_equal (dat$x, 1)
    expect_equal (dat$y, 1)
    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("distinct() works when N=2", {
    dat <- fastdf (x=1:2, y=2:1, cl=1)
    dat <- dat %>% distinct (x)
    expect_equal (sort(dat$x), 1:2)
    expect_equal (sort(dat$y), 1:2)
    expect_false (all(dat$x == dat$y))
    stopCluster (attr(dat, "cl"))
    rm (dat)

    dat <- fastdf (x=1:2, y=2:1, cl=2)
    dat <- dat %>% distinct (x)
    expect_equal (sort(dat$x), 1:2)
    expect_equal (sort(dat$y), 1:2)
    expect_false (all(dat$x == dat$y))
    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("distinct() works when local N=2", {
    dat <- fastdf (x=1:4, G=c(1,1,2,2), H=c(1,2,1,2), cl=2)
    dat <- dat %>% distinct (G, H)
    expect_equal (sort(dat$x), 1:4)
    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("group_by() can group by one level", {
    dat <- fastdf (x=1:100, G=rep(1:4, each=25), cl=2)
    dat <- dat %>% group_by (G)

    groupcol <- match (".group", attr(dat, "colnames"))
    Gcol <- match ("G", attr(dat, "colnames"))
    expect_equal (dat[[1]][, groupcol], dat[[1]][, Gcol])

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("group_by() can group by multiple levels", {
    dat <- fastdf (x=1:100, G=rep(1:4, each=25), H=rep(1:5, length.out=100), cl=2)
    dat <- dat %>% group_by (G, H)
    groupcol <- match (".group", attr(dat, "colnames"))
    expect_equal (dat[[1]][, groupcol], rep(1:20, each=5))
    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("group_by() can group uneven sizes", {
    dat <- fastdf (x=1:13,
                   G=rep(1:2, length.out=13),
                   cl=2)
    dat <- dat %>% group_by (G)
    groupcol <- match (".group", attr(dat, "colnames"))
    Gcol <- match ("G", attr(dat, "colnames"))
    expect_equal (dat[[1]][, groupcol], dat[[1]][, Gcol])

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("group_by() can group when size of group=1", {
    dat <- fastdf (x=1:4, G=c(1,1,2,2), H=c(1,2,1,2), cl=2)
    dat <- dat %>% group_by (G, H)

    groupcol <- match (".group", attr(dat, "colnames"))
    expect_equal (dat[[1]][, groupcol], 1:4)

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("group_by() can group a single item", {
    dat <- fastdf (x=1, G=1, H=1, cl=2)
    dat <- dat %>% group_by (G)
    groupcol <- match (".group", attr(dat, "colnames"))
    expect_equal (dat[[1]][, groupcol], 1)

    dat <- dat %>% group_by (G, H)
    groupcol <- match (".group", attr(dat, "colnames"))
    expect_equal (dat[[1]][, groupcol], 1)

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("group_by() maintains data integrity across cluster nodes", {
    dat <- fastdf (x=1:10, y=10:1, G=rep(1:5, each=2), cl=2)
    dat <- dat %>% partition_group (G)

    res.x <- do.call (c, dat %>% cldo ({
        out <- c()
        for (i in 1:length(.groups)) {
            out <- c(out, .grouped[[i]][, "x"])
        }
        out
    }))
    res.y <- do.call (c, dat %>% cldo ({
        out <- c()
        for (i in 1:length(.groups)) {
            out <- c(out, .grouped[[i]][, "y"])
        }
        out
    }))
    res.G <- do.call (c, dat %>% cldo ({
        out <- c()
        for (i in 1:length(.groups)) {
            out <- c(out, .grouped[[i]][, "G"])
        }
        out
    }))

    o <- order(res.G)
    expect_equal (res.x[o], 1:10)
    expect_equal (res.y[o], 10:1)
    expect_equal (res.G[o], rep(1:5, each=2))

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("group_sizes() works appropriately", {
    dat <- fastdf (x=1:100, G=rep(1, 100), H=rep(1:2, each=50),
                   I=rep(1:4, each=25), cl=2)

    dat <- dat %>% group_by (G)
    expect_equal (group_sizes(dat), rep(100, 1))
    dat <- dat %>% group_by (H)
    expect_equal (group_sizes(dat), rep(50, 2))
    dat <- dat %>% group_by (I)
    expect_equal (group_sizes(dat), rep(25, 4))

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("ungroup() works appropriately after group_by()", {
    dat <- fastdf (x=1:100, G=rep(1, 100), H=rep(1:2, each=50),
                   I=rep(1:4, each=25), cl=2)
    dat <- dat %>% group_by (G) %>% ungroup()
    dat <- dat %>% summarise (x=length(x))
    expect_equal (dat$x, 100)
    stopCluster (attr(dat, "cl"))
    rm (dat)
})
test_that ("ungroup() works appropriately after partition_group()", {
    dat <- fastdf (x=1:100, G=rep(c("A", "B", "C", "D"), each=25), cl=2)
    dat <- dat %>% partition_group (G) %>% ungroup()
    dat <- dat %>% summarise (x=length(x))
    expect_equal (dat$x, 100)
    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("regroup() works appropriately", {
    dat <- fastdf (x=1:100, G=rep(c("A", "B", "C", "D"), each=25), cl=2)
    dat <- dat %>% partition_group (G) %>% ungroup() %>% regroup()
    dat <- dat %>% summarise (x=length(x))
    expect_equal (dat$x, rep(25, 4))
    stopCluster (attr(dat, "cl"))
    rm (dat)
})

