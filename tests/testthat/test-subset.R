context("subset")

#define
#undefine
#select
#rename

cl2 <- parallel::makeCluster(2)

test_that ("define() can create new variables", {
    dat <- Multiplyr (x=1:100, alloc=2, cl=cl2)
    dat %>% define (a, b)

    expect_true (all(c("a", "b") %in% dat$col.names))

    dat["a"] <- 1:100
    dat["b"] <- 100:1
    expect_equal (dat["a"], 1:100)
    expect_equal (dat["b"], 100:1)

    rm (dat)
})

test_that ("define() can copy an existing factor", {
    dat <- Multiplyr (x=rep(c("A", "B", "C", "D"), each=25),
                   y=as.factor(rep(c("A", "B", "C", "D"), each=25)),
                   alloc=2, cl=cl2)
    dat %>% define (a=x, b=y)

    expect_true (all(c("a", "b") %in% dat$col.names))
    dat["a"] <- dat["x"]
    dat["b"] <- rev(dat["x"])
    expect_equal (dat["a"], dat["x"])
    expect_equal (dat["b"], rev(as.factor(dat["x"])))

    rm (dat)
})

test_that ("define() propagates to clusters/groups", {
    dat <- Multiplyr (x=rep(c("A", "B"), each=50),
                   G=rep(c("A", "B"), each=50), alloc=2, cl=cl2)
    dat %>% partition_group (G)

    dat %>% define (a=x)
    dat %>% transmute (a="A")
    expect_equal (dat["a"], rep("A", each=100))

    rm (dat)
})

test_that ("define() throws an error for existing/unspecified columns or non-Multiplyr", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    expect_error (dat %>% define(x))
    expect_error (dat %>% define(x,y))
    expect_error (dat %>% define())
    expect_error (data.frame(x=1:100) %>% define(x), "Multiplyr")
})

test_that ("undefine() can drop a column", {
    dat <- Multiplyr (w=1:100, x=1:100, y=100:1, z=rep("A",100), cl=cl2)
    dat %>% undefine(w)
    expect_error (dat["w"])
    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)
    expect_equal (dat["z"], rep("A", 100))

    dat %>% undefine(x, y)
    expect_error (dat["w"])
    expect_error (dat["x"])
    expect_error (dat["y"])
    expect_equal (dat["z"], rep("A", 100))

    rm (dat)
})

test_that ("undefine() throws an error for existing/unspecified columns or non-Multiplyr", {
    dat <- Multiplyr (x=1:100, y=1:100, cl=cl2)
    expect_error (dat %>% undefine(), "operations")
    expect_error (dat %>% undefine(nonexistent), "Undefined")
    expect_error (data.frame(x=1:100) %>% undefine(x), "Multiplyr")
})

test_that ("select() drops columns", {
    dat <- Multiplyr (w=1:100, x=1:100, y=100:1, z=rep("A",100), cl=cl2)
    dat %>% select (x, y)

    expect_error (dat["w"])
    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)
    expect_error (dat["z"])

    rm (dat)
})

test_that ("select() throws errors for no parameters/non-Multiplyr", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    expect_error (dat %>% select(), "columns")
    expect_error (dat %>% select(nonexistent), "Undefined")
    expect_error (data.frame(x=1:100) %>% select(x), "Multiplyr")
    rm (dat)
})

test_that ("rename() preserves data and only renames", {
    dat <- Multiplyr (w=1:100, x=1:100, y=100:1, z=rep("A",100), cl=cl2)
    dat %>% rename (a=x, b=y)

    expect_error (dat["x"])
    expect_error (dat["y"])
    expect_equal (dat["w"], 1:100)
    expect_equal (dat["a"], 1:100)
    expect_equal (dat["b"], 100:1)
    expect_equal (dat["z"], rep("A", 100))

    rm (dat)
})

test_that ("rename() propagates to clusters/groups", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), alloc=1, cl=cl2)
    dat %>% partition_group (G)

    dat %>% rename (a=x)
    dat %>% transmute (y=a)
    expect_equal (dat["y"], 1:100)

    rm (dat)
})

test_that ("rename() throws errors for no parameters/non-Multiplyr", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    expect_error (dat %>% rename(), "operations")
    expect_error (dat %>% rename(newname=nonexistent), "Undefined")
    expect_error (data.frame(x=1:100) %>% rename(y=x), "Multiplyr")
    rm (dat)
})

test_that ("slice() works on ungrouped data", {
    dat <- Multiplyr (x=1:100, y=100:1, cl=cl2)
    dat %>% slice(start = 1, end = 50)
    expect_equal (dat["x"], 1:50)
    expect_equal (dat["y"], 100:51)

    dat %>% slice(1:5)
    expect_equal (dat["x"], 1:5)
    expect_equal (dat["y"], 100:96)

    dat %>% slice(5)
    expect_equal (dat["x"], 5)
    expect_equal (dat["y"], 96)

    rm (dat)
})

test_that ("slice() works with grouped data", {
    dat <- Multiplyr (x=1:100, y=100:1,
                   G=rep(c("A", "B"), each=50), cl=cl2)
    dat %>% partition_group (G)

    dat %>% slice (start=1, end=25, each=TRUE)
    expect_equal (dat["x"], c(1:25, 51:75))
    expect_equal (dat["y"], c(100:76, 50:26))

    dat %>% slice (1:5, each=TRUE)
    expect_equal (dat["x"], c(1:5, 51:55))
    expect_equal (dat["y"], c(100:96, 50:46))

    dat %>% slice (5, each=TRUE)
    expect_equal (dat["x"], c(5, 55))
    expect_equal (dat["y"], c(96, 46))

    rm (dat)
})

test_that ("slice() throws errors for missing parameters/non-Multiplyr", {
    dat <- Multiplyr (x=1:100, y=100:1,
                      G=rep(c("A", "B"), each=50), cl=cl2)
    expect_error (dat %>% slice(), "either")
    expect_error (dat %>% slice(start=1), "either")
    expect_error (dat %>% slice(end=5), "either")
    expect_error (dat %>% slice(start=1,end=5,rows=1:5), "not both")
    expect_error (data.frame(x=1:100) %>% slice(1:5), "Multiplyr")
    rm (dat)
})

parallel::stopCluster(cl2)
