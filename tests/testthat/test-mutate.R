context("mutate")

#mutate
#transmute

cl2 <- parallel::makeCluster(2)

test_that("mutate(x=x*2) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=cl2)
    dat %>% mutate (x=x*2)
    expect_equal (dat["x"], 2*(1:100))
    rm (dat)
})

test_that("mutate(x=123) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=cl2)
    dat %>% mutate (x=123)
    expect_equal (dat["x"], rep(123, 100))
    rm (dat)
})

test_that("mutate(x=1:50) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=cl2)
    dat %>% mutate (x=1:50)
    expect_equal (dat["x"], rep(1:50, length.out=100))
    rm (dat)
})

test_that("mutate(x=123, y=x) works", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), alloc=1, cl=cl2)
    dat %>% mutate (x=123, y=x)
    expect_equal (dat["y"], rep(123, length.out=100))
    rm (dat)
})

test_that("mutate(x=length(x)) works within cluster", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=cl2)
    dat %>% mutate (x=length(x))
    expect_equal (dat["x"], rep(50, 100))
    rm (dat)
})

test_that("mutate(x=length(x)) works within group", {
    dat <- Multiplyr (x=1:100, G=rep(1:5, each=20), cl=cl2)
    dat %>% partition_group(G) %>% mutate (x=length(x))
    expect_equal (dat["x"], rep(20, 100))
    rm (dat)
})

test_that("mutate(x=\"A\") works", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50), G=rep(1:5, each=20), cl=cl2)
    dat %>% partition_group (G)
    dat %>% mutate (x="A")
    expect_equal (dat["x"], rep("A", 100))
    rm (dat)
})

test_that ("mutate() on a group column throws an error", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50), G=rep(1:5, each=20), cl=cl2)
    dat %>% group_by (G)
    expect_error (dat %>% mutate (G=3), "group column")
    rm (dat)
})

test_that ("mutate() on a former group column prevents regroup()", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50), G=rep(1:5, each=20), cl=cl2)
    dat %>% group_by (G) %>% ungroup()
    dat %>% mutate (G=3)
    expect_error (dat %>% regroup())
    rm (dat)
})

test_that ("mutate() throws error with no parameters or non-Multiplyr object", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    expect_error (dat %>% mutate(), "mutation")
    expect_error (data.frame(x=1:100) %>% mutate(x=x*2), "Multiplyr")
    rm (dat)
})

test_that("transmute(x=123) works", {
    dat <- Multiplyr (x=1:100, cl=cl2)
    dat %>% transmute (x=123)
    expect_equal (dat["x"], rep(123, 100))
    rm (dat)
})

test_that("transmute(x=1:50) works", {
    dat <- Multiplyr (x=1:100, cl=cl2)
    dat %>% transmute (x=1:50)
    expect_equal (dat["x"], rep(1:50, length.out=100))
    rm (dat)
})

test_that("transmute(x=x*2) works", {
    dat <- Multiplyr (x=1:100, cl=cl2)
    dat %>% transmute (x=x*2)
    expect_equal (dat["x"], 2*(1:100))
    rm (dat)
})

test_that("transmute(y=x) and transmute(x=y) works", {
    dat <- Multiplyr (x=1:100, alloc=1, cl=cl2)
    x <- dat["x"]

    dat %>% transmute (y=x)
    expect_equal (dat["y"], x)

    dat %>% transmute (x=y)
    expect_equal (dat["x"], x)

    rm (dat)
})

test_that("transmute(x=123, y=x) works", {
    dat <- Multiplyr (x=1:100, alloc=1, cl=cl2)
    dat %>% transmute (x=123, y=x)
    expect_equal (dat["y"], rep(123, 100))
    rm (dat)
})

test_that ("transmute(x=length(x)) works within a cluster", {
    dat <- Multiplyr (x=1:100, cl=cl2)
    dat %>% transmute(x=length(x))
    expect_equal (dat["x"], rep(50, 100))
    rm (dat)
})

test_that("transmute(x=\"A\") works", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50), G=rep(1:5, each=20), cl=cl2)
    dat %>% partition_group (G)
    dat %>% transmute (x="A")
    expect_equal (dat["x"], rep("A", 100))
    rm (dat)
})

test_that ("transmute() on a group column throws an error", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50), G=rep(1:5, each=20), cl=cl2)
    dat %>% group_by (G)
    expect_error (dat %>% transmute (G=3), "group column")
    rm (dat)
})

test_that ("transmute() throws error with no parameters or non-Multiplyr object", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    expect_error (dat %>% transmute(), "mutation")
    expect_error (data.frame(x=1:100) %>% transmute(x=x*2), "Multiplyr")
    rm (dat)
})

#Attempt to stop "no function to return from, jumping to top level"
gc()

parallel::stopCluster(cl2)
