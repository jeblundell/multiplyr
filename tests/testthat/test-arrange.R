context("arrange")

test_that ("arrange() sorts on a single cluster", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c(1,2),each=50), cl=1)
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
    stopCluster (dat$cls)
    rm (dat)
})

test_that ("arrange() can sort with multiple levels", {
    dat <- Multiplyr (x=100:1,
                   A=rep(c(2,1),each=50),
                   B=rep(c(2,1,4,3), each=25),
                   cl=1)
    dat %>% arrange (A, B, x)
    expect_equal (dat["A"], rep(c(1,2), each=50))
    expect_equal (dat["B"], rep(c(3, 4, 1, 2), each=25))
    expect_equal (dat["x"], 1:100)
    stopCluster (dat$cls)
    rm (dat)
})

test_that ("arrange() sorts on a cluster of size 2", {
    dat <- Multiplyr (x=1:100, y=100:1, cl=2)

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

    stopCluster (dat$cls)
    rm (dat)
})

test_that ("arrange() maintains groups when sorting a grouped data frame", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D", each=25)), cl=2)
    dat %>% partition_group (G)
    dat %>% arrange (y)

    for (i in c("A", "B", "C", "D")) {
        res <- dat["x"][dat["G"] == i]
        expect_equal (res, rev(sort(res)))
        res <- dat["y"][dat["G"] == i]
        expect_equal (res, sort(res))
    }

    stopCluster (dat$cls)
    rm (dat)
})
