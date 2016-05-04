context("groups")

#distinct
#group_by
#group_sizes
#ungroup
#regroup

cl1 <- parallel::makeCluster(1)
cl2 <- parallel::makeCluster(2)

test_that ("distinct() returns unique rows", {
    for (i in list(1:100, rep(1:50, length.out=100),
                   c(rep(1:25, each=2), rep(1:25, length.out=50)))) {
        dat <- Multiplyr (x=i, cl=cl2)
        dat %>% distinct (x)
        expect_equal (sort(dat["x"]), sort(unique(i)))
        rm (dat)
    }
})

test_that ("distinct() works when N=1", {
    dat <- Multiplyr (x=1, y=1, cl=cl1)
    dat %>% distinct (x, y)
    expect_equal (dat["x"], 1)
    expect_equal (dat["y"], 1)
    rm (dat)

    dat <- Multiplyr (x=1, y=1, cl=cl2)
    dat %>% distinct (x, y)
    expect_equal (dat["x"], 1)
    expect_equal (dat["y"], 1)
    rm (dat)
})

test_that ("distinct() works when N=2", {
    dat <- Multiplyr (x=1:2, y=2:1, cl=cl1)
    dat %>% distinct (x, y)
    expect_equal (sort(dat["x"]), 1:2)
    expect_equal (sort(dat["y"]), 1:2)
    expect_false (all(dat["x"] == dat["y"]))
    rm (dat)

    dat <- Multiplyr (x=1:2, y=2:1, cl=cl2)
    dat %>% distinct (x, y)
    expect_equal (sort(dat["x"]), 1:2)
    expect_equal (sort(dat["y"]), 1:2)
    expect_false (all(dat["x"] == dat["y"]))
    rm (dat)
})

test_that ("distinct() works when local N=2", {
    dat <- Multiplyr (x=1:4, G=c(1,1,2,2), H=c(1,2,1,2), cl=cl2)
    dat %>% distinct (G, H)
    expect_equal (sort(dat["x"]), 1:4)
    rm (dat)
})

test_that ("distinct() works on empty data", {
    dat <- Multiplyr (x=1:100, cl=cl2)
    dat %>% filter (x==0)
    expect_silent (dat %>% distinct(x))
    rm (dat)
})

test_that ("distinct() works on grouped data", {
    dat <- Multiplyr (x=rep(1, 100), G=rep(1:4, each=25), cl=cl2)
    dat %>% group_by (G)
    dat %>% distinct (x)
    expect_equal (dat["x"], rep(1,4))
    expect_true (dat$grouped)
    expect_true (dat$group_partition)
    expect_equal (group_sizes(dat), rep(1, 4))
    rm (dat)
})

test_that ("distinct() throws an error with undefined columns or non-Multiplyr", {
    dat <- Multiplyr (x=rep(1, 100), G=rep(1:4, each=25), cl=cl2)
    expect_error (dat %>% distinct (nonexistent))
    expect_error (data.frame(x=1:100) %>% distinct(x), "Multiplyr")
    rm (dat)
})

test_that ("distinct() works with no parameters", {
    dat <- Multiplyr (x=rep(1:2, each=50), cl=cl2)
    dat %>% distinct()
    expect_equal (dat["x"], 1:2)
    rm (dat)

    dat <- Multiplyr (x=rep(1:2, length.out=100), G=rep(1:4, each=25), cl=cl2)
    dat %>% group_by (G)
    dat %>% distinct()
    expect_equal (dat["x"], rep(1:2, 4))
    rm (dat)
})

test_that ("group_by() can group by one level", {
    dat <- Multiplyr (x=1:100, G=rep(1:4, each=25), cl=cl2)
    dat %>% group_by (G)
    expect_equal (dat$bm[, dat$groupcol], dat[, "G"])
    rm (dat)
})

test_that ("group_by() can group by multiple levels", {
    dat <- Multiplyr (x=1:100, G=rep(1:4, each=25), H=rep(1:5, length.out=100), cl=cl2)
    dat %>% group_by (G, H)
    expect_equal (dat$bm[, dat$groupcol], rep(1:20, each=5))
    rm (dat)
})

test_that ("group_by() can group uneven sizes", {
    dat <- Multiplyr (x=1:13,
                   G=rep(1:2, length.out=13),
                   cl=cl2)
    dat %>% group_by (G)
    expect_equal (dat$bm[, dat$groupcol], dat[, "G"])
    rm (dat)
})

test_that ("group_by() can group when size of group=1", {
    dat <- Multiplyr (x=1:4, G=c(1,1,2,2), H=c(1,2,1,2), cl=cl2)
    dat %>% group_by (G, H)
    expect_equal (dat$bm[, dat$groupcol], 1:4)
    rm (dat)
})

test_that ("group_by() can group a single item", {
    dat <- Multiplyr (x=1, G=1, H=1, cl=cl2)
    dat %>% group_by (G)
    expect_equal (dat$bm[, dat$groupcol], 1)

    dat %>% group_by (G, H)
    expect_equal (dat$bm[, dat$groupcol], 1)

    rm (dat)
})

test_that ("group_by() can group an empty data frame", {
    dat <- Multiplyr (x=1:100, cl=cl2)
    dat %>% filter (x < 0)
    expect_silent (dat %>% group_by(x))
    expect_true (dat$empty)
    rm (dat)
})

test_that ("group_by() maintains data integrity across cluster nodes", {
    dat <- Multiplyr (x=1:10, y=10:1, G=rep(1:5, each=2), cl=cl2)
    dat %>% partition_group (G)

    res.x <- do.call (c, dat$cluster_eval({
        out <- c()
        for (i in 1:length(.groups)) {
            out <- c(out, .grouped[[i]][, "x"])
        }
        out
    }))
    res.y <- do.call (c, dat$cluster_eval ({
        out <- c()
        for (i in 1:length(.groups)) {
            out <- c(out, .grouped[[i]][, "y"])
        }
        out
    }))
    res.G <- do.call (c, dat$cluster_eval ({
        out <- c()
        for (i in 1:length(.groups)) {
            out <- c(out, .grouped[[i]][, "G"])
        }
        out
    }))

    o <- order(res.G)
    expect_equal (res.x[o], 1:10)
    expect_equal (res.y[o], 10:1)
    expect_equal (res.G[o], rep(1:5, each=2))

    rm (dat)
})

test_that ("group_sizes() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(1, 100), H=rep(1:2, each=50),
                   I=rep(1:4, each=25), cl=cl2)

    dat %>% group_by (G)
    expect_equal (group_sizes(dat), rep(100, 1))
    dat %>% group_by (H)
    expect_equal (group_sizes(dat), rep(50, 2))
    dat %>% group_by (I)
    expect_equal (group_sizes(dat), rep(25, 4))

    dat %>% distinct (G)
    expect_equal (group_sizes(dat), rep(1, 4))

    rm (dat)
})

test_that ("group_by() throws an error with unspecified/undefined columns or non-Multiplyr", {
    dat <- Multiplyr (x=rep(1, 100), G=rep(1:4, each=25), cl=cl2)
    expect_error (dat %>% group_by (nonexistent), "Undefined")
    expect_error (dat %>% group_by(), "No grouping")
    expect_error (data.frame(x=1:100) %>% group_by(G), "Multiplyr")
    rm (dat)
})

test_that ("ungroup() works appropriately after group_by()", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25), cl=cl2)
    dat %>% group_by (G) %>% ungroup()
    dat %>% summarise (x=length(x))
    expect_equal (dat["x"], c(50, 50))
    rm (dat)
})
test_that ("ungroup() works appropriately after partition_group()", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25), cl=cl2)
    dat %>% partition_group (G) %>% ungroup()
    dat %>% summarise (x=length(x))
    expect_equal (dat["x"], c(50, 50))
    rm (dat)
})

test_that ("regroup() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25), cl=cl2)
    dat %>% partition_group (G) %>% ungroup() %>% regroup()
    dat %>% summarise (x=length(x))
    expect_equal (dat["x"], rep(25, 4))
    rm (dat)
})

parallel::stopCluster(cl1)
parallel::stopCluster(cl2)
