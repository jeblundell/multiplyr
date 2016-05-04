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
