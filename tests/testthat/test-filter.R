context("filter")

test_that ("filter() comparators work", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x < 10)
    expect_equal (dat["x"], 1:9)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x < 90)
    expect_equal (dat["x"], 1:89)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x <= 10)
    expect_equal (dat["x"], 1:10)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x <= 90)
    expect_equal (dat["x"], 1:90)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x > 10)
    expect_equal (dat["x"], 11:100)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x > 90)
    expect_equal (dat["x"], 91:100)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x >= 10)
    expect_equal (dat["x"], 10:100)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x >= 90)
    expect_equal (dat["x"], 90:100)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x == 50)
    expect_equal (dat["x"], 50)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x != 51)
    expect_equal (dat["x"], (1:100)[-51])
    stopCluster (dat$cls)
    rm (dat)
})

test_that ("filter() combinations work", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x > 40 & y <= 50)
    expect_equal (dat["x"], 41:50)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter (x < 40 | y <= 50)
    expect_equal (dat["x"], 1:50)
    stopCluster (dat$cls)
    rm (dat)

    dat <- Multiplyr (x=1:100, y=1:100, cl=2)
    dat %>% filter ((x < 40 | y <= 50) & (x >= 10))
    expect_equal (dat["x"], 10:50)
    stopCluster (dat$cls)
    rm (dat)
})

