context("arrange")

cl1 <- parallel::makeCluster(1)
cl2 <- parallel::makeCluster(2)

test_that ("arrange() sorts on a single cluster", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c(1,2),each=50), cl=cl1)
    dat %>% arrange (y)
    expect_equal (dat["x"], 100:1)
    expect_equal (dat["y"], 1:100)
    expect_equal (dat["G"], rep(c(2, 1), each=50))

    dat %>% arrange(x)
    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)
    expect_equal (dat["G"], rep(c(1, 2), each=50))

    dat %>% arrange(G, y)
    expect_equal (dat["x"], c(50:1, 100:51))
    expect_equal (dat["y"], c(51:100, 1:50))
    expect_equal (dat["G"], rep(c(1, 2), each=50))

    rm (dat)
})

test_that ("arrange() can sort with multiple levels", {
    dat <- Multiplyr (x=100:1,
                   A=rep(c(2,1),each=50),
                   B=rep(c(2,1,4,3), each=25),
                   cl=cl1)
    dat %>% arrange (A, B, x)
    expect_equal (dat["A"], rep(c(1,2), each=50))
    expect_equal (dat["B"], rep(c(3, 4, 1, 2), each=25))
    expect_equal (dat["x"], 1:100)
    rm (dat)
})

test_that ("arrange() sorts on a cluster of size 2", {
    dat <- Multiplyr (x=1:100, y=100:1, cl=cl2)

    dat %>% arrange (y)
    # test that .local$y is sorted
    res <- dat$cluster_eval (.local["y"])
    expect_equal (res[[1]], sort(res[[1]]))
    expect_equal (res[[2]], sort(res[[2]]))
    # test that .local$x is reverse sorted
    res <- dat$cluster_eval (.local["x"])
    expect_equal (res[[1]], sort(res[[1]], decreasing = TRUE))
    expect_equal (res[[2]], sort(res[[2]], decreasing = TRUE))

    dat %>% arrange (x)
    # test that .local$x is sorted
    res <- dat$cluster_eval (.local["x"])
    expect_equal (res[[1]], sort(res[[1]]))
    expect_equal (res[[2]], sort(res[[2]]))
    # test that .local$y is reverse sorted
    res <- dat$cluster_eval (.local["y"])
    expect_equal (res[[1]], sort(res[[1]], decreasing = TRUE))
    expect_equal (res[[2]], sort(res[[2]], decreasing = TRUE))
    rm (dat)
})

test_that ("arrange() maintains groups when sorting a grouped data frame", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D", each=25)), cl=cl2)
    dat %>% partition_group (G)
    dat %>% arrange (y)

    for (i in c("A", "B", "C", "D")) {
        res <- dat["x"][dat["G"] == i]
        expect_equal (res, rev(sort(res)))
        res <- dat["y"][dat["G"] == i]
        expect_equal (res, sort(res))
    }
    rm (dat)
})

test_that ("arrange() throws an error with undefined columns", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D", each=25)), cl=cl2)
    expect_error (dat %>% arrange(nonexistent))
    rm (dat)
})

test_that ("arrange() can deal with an empty data frame", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D", each=25)), cl=cl2)
    dat %>% filter(x<0)
    expect_equal (dat %>% arrange(x), dat)
    rm (dat)
})

test_that ("arrange() with no parameters returns data frame unchanged", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D", each=25)), cl=cl2)
    expect_equal (dat %>% arrange(), dat)
    rm (dat)
})

test_that ("arrange() returns a data frame", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D", each=25)), cl=cl2)
    expect_equal (dat %>% arrange(), dat)

    dat %>% arrange(x)
    expect_equal (dat %>% arrange(x), dat)
    rm (dat)
})

test_that ("arrange() throws an error for non-Multiplyr objects", {
    expect_error (data.frame(x=1:100) %>% arrange(x), "Multiplyr")
})

test_that ("arrange() can sort descending", {
    dat <- Multiplyr (x=sample(1:100, 100), cl=cl2)
    dat %>% arrange (desc(x))
    expect_equal (dat["x"], 100:1)
    rm (dat)
})

test_that ("arrange() can sort with mixed orders", {
    dat <- Multiplyr (x=c(1, 1, 1, 2, 2, 2, 3, 3, 3),
                      y=c(2, 1, 1, 2, 1, 1, 2, 1, 1),
                      z=c(1, 2, 3, 1, 2, 3, 1, 2, 3), cl=cl2)
    dat %>% arrange (desc(x), y, desc(z))
    expect_equal (dat["x"], c(3, 3, 3, 2, 2, 2, 1, 1, 1))
    expect_equal (dat["y"], c(1, 1, 2, 1, 1, 2, 1, 1, 2))
    expect_equal (dat["z"], c(3, 2, 1, 3, 2, 1, 3, 2, 1))
    rm (dat)
})

test_that ("arrange() throws an error for inappropriate sorting expressions", {
    dat <- Multiplyr (x=c(1, 1, 1, 2, 2, 2, 3, 3, 3),
                      y=c(2, 1, 1, 2, 1, 1, 2, 1, 1),
                      z=c(1, 2, 3, 1, 2, 3, 1, 2, 3), cl=cl2)
    expect_error (dat %>% arrange(sum(x)), "sorting expression")
    expect_error (dat %>% arrange(desc(x, y)), "sorting expression")
    rm (dat)
})

#Attempt to stop "no function to return from, jumping to top level"
gc()

parallel::stopCluster (cl1)
parallel::stopCluster (cl2)
