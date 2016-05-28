context("filter")

cl2 <- parallel::makeCluster(2)

test_that ("filter() comparators work", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x < 10)
    expect_equal (dat["x"], 1:9)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x < 90)
    expect_equal (dat["x"], 1:89)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x <= 10)
    expect_equal (dat["x"], 1:10)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x <= 90)
    expect_equal (dat["x"], 1:90)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x > 10)
    expect_equal (dat["x"], 11:100)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x > 90)
    expect_equal (dat["x"], 91:100)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x >= 10)
    expect_equal (dat["x"], 10:100)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x >= 90)
    expect_equal (dat["x"], 90:100)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x == 50)
    expect_equal (dat["x"], 50)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x != 51)
    expect_equal (dat["x"], (1:100)[-51])
    rm (dat)
})

test_that ("filter() combinations work", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x > 40 & y <= 50)
    expect_equal (dat["x"], 41:50)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x < 40 | y <= 50)
    expect_equal (dat["x"], 1:50)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter ((x < 40 | y <= 50) & (x >= 10))
    expect_equal (dat["x"], 10:50)
    rm (dat)
})

test_that ("filter() can return an empty result", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    expect_silent (dat %>% filter(x<0))
    expect_true (dat$empty)
    rm (dat)
})

test_that ("filter() can cope with an empty data frame", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    dat %>% filter (x<0)
    expect_silent (dat %>% filter(x>50))
    rm (dat)
})

test_that ("filter() throws errors with no criteria or non-Multiplyr object", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    expect_error (dat %>% filter(), "criteria")
    expect_error (data.frame(x=1:100) %>% filter(), "Multiplyr")
    rm (dat)
})

test_that ("filter() updates group sizes", {
    dat <- Multiplyr (x=1:100, G=rep(1:2, each=50), cl=cl2)
    dat %>% group_by (G)
    dat %>% filter (x<=50)
    expect_equal (group_sizes(dat), c(50, 0))
})

#Attempt to stop "no function to return from, jumping to top level"
gc()

parallel::stopCluster (cl2)

