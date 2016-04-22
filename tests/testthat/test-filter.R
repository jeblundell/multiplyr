context("filter")

.unfilter <- function (x) {
    filtercol <- match (".filter", attr(x, "colnames"))
    x[[1]][, filtercol] <- 1
}

dat <- fastdf (x=1:100, y=1:100, cl=2)

test_that ("fast_filter() comparators work", {
    dat <- dat %>% fast_filter (x < 10)
    expect_equal (dat$x, 1:9)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x < 90)
    expect_equal (dat$x, 1:89)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x <= 10)
    expect_equal (dat$x, 1:10)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x <= 90)
    expect_equal (dat$x, 1:90)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x > 10)
    expect_equal (dat$x, 11:100)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x > 90)
    expect_equal (dat$x, 91:100)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x >= 10)
    expect_equal (dat$x, 10:100)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x >= 90)
    expect_equal (dat$x, 90:100)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x == 50)
    expect_equal (dat$x, 50)
    .unfilter (dat)

    dat <- dat %>% fast_filter (x != 51)
    expect_equal (dat$x, (1:100)[-51])
    .unfilter (dat)
})

test_that ("fast_filter() combinations work", {
    dat <- dat %>% fast_filter (x > 40, y < 50)
    expect_equal (dat$x, 41:49)
    expect_equal (dat$y, 41:49)
    .unfilter (dat)
})

test_that ("filter() comparators work", {
    dat <- dat %>% filter (x < 10)
    expect_equal (dat$x, 1:9)
    .unfilter (dat)

    dat <- dat %>% filter (x < 90)
    expect_equal (dat$x, 1:89)
    .unfilter (dat)

    dat <- dat %>% filter (x <= 10)
    expect_equal (dat$x, 1:10)
    .unfilter (dat)

    dat <- dat %>% filter (x <= 90)
    expect_equal (dat$x, 1:90)
    .unfilter (dat)

    dat <- dat %>% filter (x > 10)
    expect_equal (dat$x, 11:100)
    .unfilter (dat)

    dat <- dat %>% filter (x > 90)
    expect_equal (dat$x, 91:100)
    .unfilter (dat)

    dat <- dat %>% filter (x >= 10)
    expect_equal (dat$x, 10:100)
    .unfilter (dat)

    dat <- dat %>% filter (x >= 90)
    expect_equal (dat$x, 90:100)
    .unfilter (dat)

    dat <- dat %>% filter (x == 50)
    expect_equal (dat$x, 50)
    .unfilter (dat)

    dat <- dat %>% filter (x != 51)
    expect_equal (dat$x, (1:100)[-51])
    .unfilter (dat)
})

test_that ("filter() combinations work", {
    dat <- dat %>% filter (x > 40 & y <= 50)
    expect_equal (dat$x, 41:50)
    .unfilter (dat)

    dat <- dat %>% filter (x < 40 | y <= 50)
    expect_equal (dat$x, 1:50)
    .unfilter (dat)

    dat <- dat %>% filter ((x < 40 | y <= 50) & (x >= 10))
    expect_equal (dat$x, 10:50)
    .unfilter (dat)
})

stopCluster (attr(dat, "cl"))
