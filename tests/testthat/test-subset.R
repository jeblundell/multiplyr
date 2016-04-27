context("subset")

test_that ("define() can create new variables", {
    dat <- fastdf (x=1:100, alloc=2, cl=2)
    dat <- dat %>% define (a, b)

    expect_true (all(c("a", "b") %in% attr(dat, "colnames")))

    dat$a <- dat$x
    dat$b <- rev(dat$x)
    expect_equal (dat$a, dat$x)
    expect_equal (dat$b, rev(dat$x))

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("define() can copy an existing factor", {
    dat <- fastdf (x=rep(c("A", "B", "C", "D"), each=25),
                   y=as.factor(rep(c("A", "B", "C", "D"), each=25)),
                   alloc=2, cl=2)
    dat <- dat %>% define (a=x, b=y)

    expect_true (all(c("a", "b") %in% attr(dat, "colnames")))
    dat$a <- dat$x
    dat$b <- rev(dat$x)
    expect_equal (dat$a, dat$x)
    expect_equal (dat$b, rev(as.factor(dat$x)))

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("undefine() can drop a column", {
    dat <- fastdf (w=1:100, x=1:100, y=100:1, z=rep("A",100), cl=2)
    dat <- dat %>% undefine(w)
    expect_error (dat$w)
    expect_equal (dat$x, 1:100)
    expect_equal (dat$y, 100:1)
    expect_equal (dat$z, rep("A", 100))

    dat <- dat %>% undefine(x, y)
    expect_error (dat$w)
    expect_error (dat$x)
    expect_error (dat$y)
    expect_equal (dat$z, rep("A", 100))

    stopCluster (attr(dat, "cl"))
    rm (dat)

})

test_that ("select() drops columns", {
    dat <- fastdf (w=1:100, x=1:100, y=100:1, z=rep("A",100), cl=2)
    dat <- dat %>% select (x, y)

    expect_equal (dat$x, 1:100)
    expect_equal (dat$y, 100:1)

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("rename() preserves data and only renames", {
    dat <- fastdf (w=1:100, x=1:100, y=100:1, z=rep("A",100), cl=2)
    dat <- dat %>% rename (a=x, b=y)

    expect_error (dat$x)
    expect_error (dat$y)
    expect_equal (dat$w, 1:100)
    expect_equal (dat$a, 1:100)
    expect_equal (dat$b, 100:1)
    expect_equal (dat$z, rep("A", 100))

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("slice() works on ungrouped data", {
    dat <- fastdf (x=1:100, y=100:1, cl=2)
    dat <- dat %>% slice(start = 1, end = 50)
    expect_equal (dat$x, 1:50)
    expect_equal (dat$y, 100:51)

    dat <- dat %>% slice(1:5)
    expect_equal (dat$x, 1:5)
    expect_equal (dat$y, 100:96)

    dat <- dat %>% slice(5)
    expect_equal (dat$x, 5)
    expect_equal (dat$y, 96)

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

test_that ("slice() works with grouped data", {
    dat <- fastdf (x=1:100, y=100:1,
                   G=rep(c("A", "B"), each=50), cl=2)
    dat <- dat %>% partition_group (G)

    dat <- dat %>% slice (start=1, end=25)
    expect_equal (dat$x, c(1:25, 51:75))
    expect_equal (dat$y, c(100:76, 50:26))

    dat <- dat %>% slice (1:5)
    expect_equal (dat$x, c(1:5, 51:55))
    expect_equal (dat$y, c(100:96, 50:46))

    dat <- dat %>% slice (5)
    expect_equal (dat$x, c(5, 55))
    expect_equal (dat$y, c(96, 46))

    stopCluster (attr(dat, "cl"))
    rm (dat)
})
