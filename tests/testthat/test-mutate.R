context("mutate")

test_that("mutate(x=x*2) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=2)
    dat %>% mutate (x=x*2)
    expect_equal (dat["x"], 2*(1:100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("mutate(x=123) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=2)
    dat %>% mutate (x=123)
    expect_equal (dat["x"], rep(123, 100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("mutate(x=1:50) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=2)
    dat %>% mutate (x=1:50)
    expect_equal (dat["x"], rep(1:50, length.out=100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("mutate(x=123, y=x) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=2)
    dat %>% mutate (x=123, y=x)
    expect_equal (dat["y"], rep(123, length.out=100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("mutate(x=length(x)) works within cluster", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=2)
    dat %>% mutate (x=length(x))
    expect_equal (dat["x"], rep(50, 100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("mutate(x=length(x)) works within group", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=2)
    dat %>% partition_group(G) %>% mutate (x=length(x))
    expect_equal (dat["x"], rep(20, 100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("mutate(x=\"A\") works", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50), G=rep(1:5, each=20), cl=2)
    dat %>% partition_group (G)
    dat %>% mutate (x="A")
    expect_equal (dat["x"], rep("A", 100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("transmute(x=123) works", {
    dat <- Multiplyr (x=1:100, cl=2)
    dat %>% transmute (x=123)
    expect_equal (dat["x"], rep(123, 100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("transmute(x=1:50) works", {
    dat <- Multiplyr (x=1:100, cl=2)
    dat %>% transmute (x=1:50)
    expect_equal (dat["x"], rep(1:50, length.out=100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("transmute(x=x*2) works", {
    dat <- Multiplyr (x=1:100, cl=2)
    dat %>% transmute (x=x*2)
    expect_equal (dat["x"], 2*(1:100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("transmute(y=x) and transmute(x=y) works", {
    dat <- Multiplyr (x=1:100, cl=2)
    x <- dat["x"]

    dat %>% transmute (y=x)
    expect_equal (dat["y"], x)

    dat %>% transmute (x=y)
    expect_equal (dat["x"], x)

        stopCluster(dat$cls)
    rm (dat)
})

test_that("transmute(x=123, y=x) works", {
    dat <- Multiplyr (x=1:100, cl=2)
    dat %>% transmute (x=123, y=x)
    expect_equal (dat["y"], rep(123, 100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that ("transmute(x=length(x)) works within a cluster", {
    dat <- Multiplyr (x=1:100, cl=2)
    dat %>% transmute(x=length(x))
    expect_equal (dat["x"], rep(50, 100))
    stopCluster(dat$cls)
    rm (dat)
})

test_that("transmute(x=\"A\") works", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50), G=rep(1:5, each=20), cl=2)
    dat %>% partition_group (G)
    dat %>% transmute (x="A")
    expect_equal (dat["x"], rep("A", 100))
    stopCluster(dat$cls)
    rm (dat)
})
