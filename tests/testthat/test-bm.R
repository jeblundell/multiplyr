context("bm")

test_that ("bm_morder() works like bigmemory::morder with single column sorting", {
    bm <- bigmemory::big.matrix (9, 3)
    bm[, 1] <- c(1, 1, 1, 2, 2, 2, 3, 3, 3)
    bm[, 2] <- c(2, 1, 1, 2, 1, 1, 2, 1, 1)
    bm[, 3] <- c(1, 2, 3, 1, 2, 3, 1, 2, 3)
    expect_equal (bigmemory::morder (bm, cols=1), bm_morder (bm, cols=1))
    expect_equal (bigmemory::morder (bm, cols=1, decreasing=TRUE), bm_morder (bm, cols=1, decreasing=TRUE))
    rm (bm)
})

test_that ("bm_morder() works like bigmemory::morder with multiple column sorting", {
    bm <- bigmemory::big.matrix (9, 3)
    bm[, 1] <- c(1, 1, 1, 2, 2, 2, 3, 3, 3)
    bm[, 2] <- c(2, 1, 1, 2, 1, 1, 2, 1, 1)
    bm[, 3] <- c(1, 2, 3, 1, 2, 3, 1, 2, 3)
    expect_equal (bigmemory::morder (bm, cols=c(1,2)), bm_morder (bm, cols=c(1,2)))
    expect_equal (bigmemory::morder (bm, cols=c(2,1)), bm_morder (bm, cols=c(2,1)))
    expect_equal (bigmemory::morder (bm, cols=c(1,2,3)), bm_morder (bm, cols=c(1,2,3)))
    expect_equal (bigmemory::morder (bm, cols=c(3,2,1)), bm_morder (bm, cols=c(3,2,1)))
    expect_equal (bigmemory::morder (bm, cols=c(1,2), decreasing=TRUE), bm_morder (bm, cols=c(1,2), decreasing=TRUE))
    expect_equal (bigmemory::morder (bm, cols=c(2,1), decreasing=TRUE), bm_morder (bm, cols=c(2,1), decreasing=TRUE))
    expect_equal (bigmemory::morder (bm, cols=c(1,2,3), decreasing=TRUE), bm_morder (bm, cols=c(1,2,3), decreasing=TRUE))
    expect_equal (bigmemory::morder (bm, cols=c(3,2,1), decreasing=TRUE), bm_morder (bm, cols=c(3,2,1), decreasing=TRUE))
    rm (bm)
})

test_that ("bm_morder() can resolve a 2 column tie with an opposite sorting order", {
    bm <- bigmemory::big.matrix (9, 3)
    bm[, 1] <- c(1, 1, 1, 2, 2, 2, 3, 3, 3)
    bm[, 2] <- c(2, 1, 1, 2, 1, 1, 2, 1, 1)
    bm[, 3] <- c(1, 2, 3, 1, 2, 3, 1, 2, 3)
    expect_equal (bm_morder (bm, cols=c(1, 2), decreasing=c(FALSE, TRUE)), 1:9)
    expect_equal (bm_morder (bm, cols=c(1, 2), decreasing=c(TRUE, FALSE)), c(8, 9, 7, 5, 6, 4, 2, 3, 1))
    rm (bm)
})

test_that ("bm_morder() can resolve a 3 column tie with an opposite sorting order", {
    bm <- bigmemory::big.matrix (9, 3)
    bm[, 1] <- c(1, 1, 1, 2, 2, 2, 3, 3, 3)
    bm[, 2] <- c(2, 1, 1, 2, 1, 1, 2, 1, 1)
    bm[, 3] <- c(1, 2, 3, 1, 2, 3, 1, 2, 3)
    expect_equal (bm_morder (bm, cols=c(1, 2, 3), decreasing=c(TRUE, FALSE, FALSE)), c(8, 9, 7, 5, 6, 4, 2, 3, 1))
    expect_equal (bm_morder (bm, cols=c(1, 2, 3), decreasing=c(TRUE, FALSE, TRUE)), 9:1)
    rm (bm)
})


