context("utils")

#distribute

test_that ("distribute(x, N) returns N lots of x/N", {
    for (each in c(10, 33, 50)) {
        for (N in c(1, 3)) {
            total <- each * N
            res <- distribute (total, N)
            expect_equal (length(res), N)
            for (i in 1:N) {
                expect_equal (res[i], each)
            }
        }
    }
})

test_that ("distribute(x, N) returns even distribution when x is a vector of repeats", {
    for (G in 2:4) {
        for (sz in c(25, 33)) {
            v <- rep(sz, G)
            res <- distribute (v, G)
            expect_equal (length(res), G)
            resv <- c()
            for (i in 1:G) {
                expect_equal (length(res[[i]]), 1)
                resv <- c(resv, res[[i]])
            }
            resv <- sort(resv)
            expect_equal (resv, 1:G)
        }
    }
})

test_that ("distribute(x, N) on not evenly divisible x sums to right total", {
    for (sz in c(13, 31, 67)) {
        for (N in c(2, 3, 4)) {
            res <- distribute (sz, N)
            expect_equal (sum(res), sz)
        }
    }
})


test_that ("distribute(x, N) on not evenly divisible groups covers all groups", {
    for (sz in c(7, 11, 67)) {
        for (N in c(2, 3, 4)) {
            G <- rep(13, sz)
            res <- distribute (G, N)
            expect_equal (length(res), N)
            resv <- c()
            for (i in 1:N) {
                resv <- c(resv, res[[i]])
            }
            resv <- sort(resv)
            expect_equal (resv, 1:sz)
        }
    }
})

test_that ("between() can positively/negatively identify if a number is between two limits", {
   expect_true (between (2, 1, 3))
   expect_true (between (1, 1, 3))
   expect_true (between (3, 1, 3))
   expect_true (between (-2, -3, -1))
   expect_false (between (0, 1, 3))
   expect_false (between (4, 1, 3))
   expect_false (between (-4, -3, -2))
})

test_that ("cumall() returns cumulative all for various cases", {
   expect_equal (cumall(c(TRUE, FALSE, TRUE, FALSE, FALSE)), c(TRUE, FALSE, FALSE, FALSE, FALSE))
   expect_equal (cumall(c(TRUE, TRUE, FALSE, FALSE, TRUE)), c(TRUE, TRUE, FALSE, FALSE, FALSE))
   expect_equal (cumall(c(FALSE, TRUE, TRUE, FALSE, FALSE)), c(FALSE, FALSE, FALSE, FALSE, FALSE))
   expect_equal (cumall(TRUE), TRUE)
   expect_equal (cumall(rep(TRUE, 10)), rep(TRUE, 10))
   expect_equal (cumall(FALSE), FALSE)
   expect_equal (cumall(rep(FALSE, 10)), rep(FALSE, 10))
})

test_that ("cumany() returns cumulative any for various cases", {
   expect_equal (cumany(c(FALSE, TRUE, FALSE, FALSE, FALSE)), c(FALSE, TRUE, TRUE, TRUE, TRUE))
   expect_equal (cumany(c(TRUE, FALSE, FALSE, FALSE, FALSE)), c(TRUE, TRUE, TRUE, TRUE, TRUE))
   expect_equal (cumany(TRUE), TRUE)
   expect_equal (cumany(rep(TRUE, 10)), rep(TRUE, 10))
   expect_equal (cumany(FALSE), FALSE)
   expect_equal (cumany(rep(FALSE, 10)), rep(FALSE, 10))
})

test_that ("cummean() returns cumulative mean for various cases", {
   expect_equal (cummean(0), 0)
   expect_equal (cummean(10), 10)
   expect_equal (cummean(rep(10, 10)), rep(10, 10))
   expect_equal (cummean(1:10), seq(1, 5.5, 0.5))
   expect_equal (cummean (c(2, 4, 12, 1, 8)), c(2, 3, 6, 4.75, 5.4))
})

test_that ("first() returns the first element or default if empty", {
   expect_equal (first(1), 1)
   expect_equal (first(10:20), 10)
   expect_equal (first(numeric(0)), NA)
   expect_equal (first(numeric(0), default=123), 123)
})

test_that ("lag() can return a lag padded with default", {
   expect_equal (lag(1:5), c(NA, 1, 2, 3, 4))
   expect_equal (lag(1:5, 2), c(NA, NA, 1, 2, 3))
   expect_equal (lag(1:5, 5), rep(NA, 5))
})

test_that ("lead() can return a lead padded with default", {
   expect_equal (lead(1:5), c(2, 3, 4, 5, NA))
   expect_equal (lead(1:5, 2), c(3, 4, 5, NA, NA))
   expect_equal (lead(1:5, 5), rep(NA, 5))
})

test_that ("last() returns the last element or default if empty", {
   expect_equal (last(1), 1)
   expect_equal (last(10:20), 20)
   expect_equal (last(numeric(0)), NA)
   expect_equal (last(numeric(0), default=123), 123)
})

test_that ("n_distinct() returns number of distint items for various cases", {
    expect_equal (n_distinct(1:10), 10)
    expect_equal (n_distinct(c(rep(5, 5), rep(4, 5))), 2)
    expect_equal (n_distinct(sample(rep(1:10, each=10), 100)), 10)
})

test_that ("nonunique() returns number of distint items for various cases", {
    expect_equal (length(nonunique(1:10)), 0)
    expect_equal (nonunique(rep(1, 10)), 1)
    expect_equal (sort(nonunique(sample(rep(1:10, each=10), 100))), 1:10)
})

test_that ("nth() returns the nth element with default if not possible for various cases", {
   expect_equal (nth(1:10, 1), 1)
   expect_equal (nth(1:10, 10), 10)
   expect_equal (nth(1:10, 11), NA)
   expect_equal (nth(1:10, 11, default=123), 123)
   expect_equal (nth(numeric(0), 1, default=123), 123)
})

