context("summarise")

#summarise

cl2 <- parallel::makeCluster(2)

test_that ("summarise() works on ungrouped data", {
    dat <- Multiplyr (x=1:100, y=rep(2, 100), alloc=1, cl=cl2)
    dat %>% summarise (x=length(x), y=length(y)/2, z=sum(y))
    expect_equal (dat["x"], c(50, 50))
    expect_equal (dat["y"], c(25, 25))
    expect_equal (dat["z"], c(100, 100))
    rm (dat)
})

test_that ("summarise() works on grouped data", {
    dat <- Multiplyr (x=1:100, y=rep(2, 100),
                   G=rep(c("A", "B", "C", "D"), each=25),
                   alloc=1, cl=cl2)
    dat %>% partition_group (G)
    dat %>% summarise (x=length(x), y=length(y)/2, z=sum(y))
    expect_equal (dat["x"], rep(25, 4))
    expect_equal (dat["y"], rep(12.5, 4))
    expect_equal (dat["z"], rep(50, 4))
    rm (dat)
})

test_that ("summarise() works with transmute/rename", {
    dat <- Multiplyr (x=1:100, y=rep(2, 100),
                   G=rep(c("A", "B", "C", "D"), each=25),
                   alloc=1, cl=cl2)
    dat %>% partition_group(G)
    dat %>%
        rename(a=x, b=y) %>%
        transmute(b = b*2) %>%
        summarise(x=length(b), y=sum(b))

    expect_equal (dat["x"], rep(25, 4))
    expect_equal (dat["y"], rep(100, 4))

    rm (dat)
})

test_that ("summarise() throws an error if no parameters or non-Multiplyr", {
    dat <- Multiplyr (x=1:100, y=rep(2, 100), alloc=1, cl=cl2)
    expect_error (dat %>% summarise(), "operations")
    expect_error (data.frame(x=1:100) %>% summarise(N=length(x)), "Multiplyr")
    rm (dat)
})

test_that ("reduce() keeps the right columns", {
    dat <- Multiplyr (x=1:100, y=rep(2, 100),
                      G=rep(c("A", "B", "C", "D"), each=25),
                      alloc=1, cl=cl2)
    dat %>% group_by (G) %>% summarise (N=length(x)) %>% reduce(N=sum(N))
    expect_equal (names(dat), c("G", "N"))
    rm (dat)
})

#Attempt to stop "no function to return from, jumping to top level"
gc()

parallel::stopCluster(cl2)
