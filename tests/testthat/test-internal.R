context("internal")

test_that ("sm_desc_update() provides similar behaviour to describe", {
    bm <- bigmemory::big.matrix(100, 100)
    sm <- bigmemory::sub.big.matrix (bm, firstRow=50, lastRow=100)

    desc <- bigmemory::describe (bm)
    desc <- sm_desc_update(desc, 50, 100)
    expect_equal (desc, bigmemory::describe (sm))

    sm2 <- bigmemory::sub.big.matrix (sm, firstRow=10, lastRow=20)
    smd <- bigmemory::sub.big.matrix (desc, firstRow=10, lastRow=20)
    desc <- sm_desc_update(desc, 10, 20)
    expect_equal (desc, bigmemory::describe (sm2))
    expect_equal (smd, sm2)
})
