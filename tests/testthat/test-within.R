context("within")

cl2 <- parallel::makeCluster(2)

test_that ("within_group() can get/set a column", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)
    dat %>% group_by (G)

    dat %>% within_group ({x <- 1:50})
    expect_equal (dat["x"], rep(1:50, 2))

    dat %>% within_group ({x <- y})
    expect_equal (dat["x"], 100:1)

    rm (dat)
})

test_that ("within_group() can persist a non-column variable", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)
    dat %>% group_by (G)

    dat %>% within_group ({z <- 1:50})
    dat %>% within_group ({x <- z})
    expect_equal (dat["x"], rep(1:50, 2))

    rm (dat)
})

test_that ("within_group() can export a non-column variable", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)
    dat %>% group_by (G)

    dat %>% within_group ({z <- 1:50})
    dat %>% mutate (x=z)
    expect_equal (dat["x"], rep(1:50, 2))

    rm (dat)
})

test_that ("within_group() works with empty groups/nodes", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D"), each=25), alloc=2, cl=cl2)
    dat %>% group_by (G)

    dat %>% filter (x<=50)
    dat %>% within_group ({x <- y})
    expect_equal (dat["x"], 100:51)

    dat %>% filter (x>75)
    dat %>% within_group ({x <- rev(y)})
    expect_equal (dat["x"], 76:100)

    rm (dat)
})

test_that ("within_group() throws errors", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)
    expect_error (dat %>% within_group({z <- x}), "group_by")
    expect_error (data.frame(x=1:100) %>% within_group({z <- x}), "Multiplyr")
    rm (dat)
})

test_that ("within_node() can get/set a column", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)

    dat %>% within_node ({x <- 1:50})
    expect_equal (dat["x"], rep(1:50, 2))

    dat %>% within_node ({x <- y})
    expect_equal (dat["x"], 100:1)

    rm (dat)
})

test_that ("within_node() can persist a non-column variable", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)

    dat %>% within_node ({z <- 1:50})
    dat %>% within_node ({x <- z})
    expect_equal (dat["x"], rep(1:50, 2))

    rm (dat)
})

test_that ("within_node() can export a non-column variable", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)

    dat %>% within_node ({z <- 1:50})
    dat %>% mutate (x=z)
    expect_equal (dat["x"], rep(1:50, 2))

    rm (dat)
})

test_that ("within_group() works with empty nodes", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B", "C", "D"), each=25), alloc=2, cl=cl2)

    dat %>% filter (x<=50)
    dat %>% within_node ({x <- y})
    expect_equal (dat["x"], 100:51)

    rm (dat)
})

test_that ("within_node() throws errors", {
    expect_error (data.frame(x=1:100) %>% within_group({z <- x}), "Multiplyr")
})

parallel::stopCluster(cl2)
