context("class")

cl2 <- parallel::makeCluster(2)

test_that ("Multiplyr(x=..., y=...) creates the appropriate structure", {
    dat <- Multiplyr(x=1:100,
                  f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                  G=rep(c("A", "B", "C", "D"), each=25),
                  alloc=1,
                  cl=cl2)

    expect_equal (nrow(dat$bm), 100)
    expect_gte (ncol(dat$bm), 3)
    expect_true (class(dat) == "Multiplyr")

    expect_equal (dat$first, 1)
    expect_equal (dat$last, 100)

    expect_equal (dat$col.names[1:3], c("x", "f", "G"))
    expect_equal (dat$order.cols[1:3], c(1, 2, 3))
    expect_equal (dat$type.cols[1:3], c(0, 1, 2))

    expect_equal (sum (dat$order.cols[-(1:3)]), 0)
    expect_true (all(c(".tmp", ".filter", ".group") %in% dat$col.names))
    expect_equal (dat$tmpcol, match(".tmp", dat$col.names))
    expect_equal (dat$filtercol, match(".filter", dat$col.names))
    expect_equal (dat$groupcol, match(".group", dat$col.names))
    expect_true (all(dat$bm[, dat$filtercol] == 1))

    expect_equal (dat$factor.cols, c(2, 3))
    expect_equal (dat$factor.levels[[1]], c("f1", "f2"))
    expect_equal (dat$factor.levels[[2]], c("A", "B", "C", "D"))

    expect_gte (sum(is.na(dat$col.names)), 1)

    expect_equal (length(dat$cls), 2)
    expect_true (all(c("SOCKcluster", "cluster") %in% class(dat$cls)))

    res <- do.call (c, dat$cluster_eval (123))
    expect_equal (res, rep(123, 2))

    expect_equal (dat$group.cols, 0)
    expect_equal (dat$grouped, FALSE)
    expect_equal (dat$group, 0)
    expect_equal (dat$group_partition, FALSE)
    expect_equal (dat$empty, FALSE)

    rm (dat)
})

test_that ("Multiplyr() works on a data.frame", {
    dat.df <- data.frame(x=1:100,
                         f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                         G=rep(c("A", "B", "C", "D"), each=25),
                         stringsAsFactors = FALSE)
    dat <- Multiplyr(dat.df,
                  alloc=1,
                  cl=cl2)

    expect_equal (nrow(dat$bm), 100)
    expect_gte (ncol(dat$bm), 3)
    expect_true (class(dat) == "Multiplyr")

    expect_equal (dat$first, 1)
    expect_equal (dat$last, 100)

    expect_equal (dat$col.names[1:3], c("x", "f", "G"))
    expect_equal (dat$order.cols[1:3], c(1, 2, 3))
    expect_equal (dat$type.cols[1:3], c(0, 1, 2))

    expect_equal (sum (dat$order.cols[-(1:3)]), 0)
    expect_true (all(c(".tmp", ".filter", ".group") %in% dat$col.names))
    expect_equal (dat$tmpcol, match(".tmp", dat$col.names))
    expect_equal (dat$filtercol, match(".filter", dat$col.names))
    expect_equal (dat$groupcol, match(".group", dat$col.names))
    expect_true (all(dat$bm[, dat$filtercol] == 1))

    expect_equal (dat$factor.cols, c(2, 3))
    expect_equal (dat$factor.levels[[1]], c("f1", "f2"))
    expect_equal (dat$factor.levels[[2]], c("A", "B", "C", "D"))

    expect_gte (sum(is.na(dat$col.names)), 1)

    expect_equal (length(dat$cls), 2)
    expect_true (all(c("SOCKcluster", "cluster") %in% class(dat$cls)))

    res <- do.call (c, dat$cluster_eval (123))
    expect_equal (res, rep(123, 2))

    expect_equal (dat$group.cols, 0)
    expect_equal (dat$grouped, FALSE)
    expect_equal (dat$group, 0)
    expect_equal (dat$group_partition, FALSE)
    expect_equal (dat$empty, FALSE)

    rm (dat)
})

test_that ("dat$copy(shallow=TRUE) creates the appropriate structure", {
    tmp <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat <- tmp$copy(shallow=TRUE)

    expect_equal (nrow(dat$bm), 100)
    expect_gte (ncol(dat$bm), 3)
    expect_true (class(dat) == "Multiplyr")

    expect_equal (dat$first, 1)
    expect_equal (dat$last, 100)

    expect_equal (dat$col.names[1:3], c("x", "f", "G"))
    expect_equal (dat$order.cols[1:3], c(1, 2, 3))
    expect_equal (dat$type.cols[1:3], c(0, 1, 2))

    expect_equal (sum (dat$order.cols[-(1:3)]), 0)
    expect_true (all(c(".tmp", ".filter", ".group") %in% dat$col.names))
    expect_equal (dat$tmpcol, match(".tmp", dat$col.names))
    expect_equal (dat$filtercol, match(".filter", dat$col.names))
    expect_equal (dat$groupcol, match(".group", dat$col.names))
    expect_true (all(dat$bm[, dat$filtercol] == 1))

    expect_equal (dat$factor.cols, c(2, 3))
    expect_equal (dat$factor.levels[[1]], c("f1", "f2"))
    expect_equal (dat$factor.levels[[2]], c("A", "B", "C", "D"))

    expect_gte (sum(is.na(dat$col.names)), 1)

    expect_equal (length(dat$cls), 2)
    expect_true (all(c("SOCKcluster", "cluster") %in% class(dat$cls)))

    res <- do.call (c, dat$cluster_eval (123))
    expect_equal (res, rep(123, 2))

    expect_equal (dat$group.cols, 0)
    expect_equal (dat$grouped, FALSE)
    expect_equal (dat$group, 0)
    expect_equal (dat$group_partition, FALSE)

    rm (dat)
})

N<-100
dat.df <- data.frame (x=N:1,
             animal=rep(c("dog", "cat", "mouse", "cow"), each=N/4),
             location=as.factor(rep(c("house", "field", "space"), length.out=N)),
             stringsAsFactors = FALSE)
dat <- Multiplyr (dat.df, alloc=1, cl=cl2)

test_that ("Multiplyr(x=...)[\"x\"] returns ...", {
    expect_equal (dat["x"], dat.df$x)
    expect_equal (dat["animal"], dat.df$animal)
    expect_equal (dat["location"], dat.df$location)
    expect_equal (levels(dat["location"]), levels(dat.df$location))

    expect_true (is.numeric(dat["x"]))
    expect_false (is.numeric(dat["animal"]))
    expect_false (is.numeric(dat["location"]))

    expect_false (is.factor(dat["x"]))
    expect_false (is.factor(dat["animal"]))
    expect_true (is.factor(dat["location"]))

    expect_false (is.character(dat["x"]))
    expect_true (is.character(dat["animal"]))
    expect_false (is.character(dat["location"]))
})

test_that ("Multiplyr[x, y] subsets rows and columns appropriately", {
    #columns
    expect_equal (dat[,"x"], dat.df$x)
    expect_equal (dat[,"animal"], dat.df$animal)
    expect_equal (dat[,"location"], dat.df$location)
    expect_equal (levels(dat[,"location"]), levels(dat.df$location))

    expect_true (is.numeric(dat[,"x"]))
    expect_false (is.numeric(dat[,"animal"]))
    expect_false (is.numeric(dat[,"location"]))

    expect_false (is.factor(dat[,"x"]))
    expect_false (is.factor(dat[,"animal"]))
    expect_true (is.factor(dat[,"location"]))

    expect_false (is.character(dat[,"x"]))
    expect_true (is.character(dat[,"animal"]))
    expect_false (is.character(dat[,"location"]))

    # multiple columns
    expect_true (all (dat[, c("x", "animal", "location")] == dat.df))
    expect_true (all (dat[, c("location", "x", "animal")] ==
                          dat.df[, c("location", "x", "animal")]))

    #rows
    for (rowslice in list(1:50, 50:100, 1, 100, c(20, 40, 60, 80))) {
        expect_equal (dat[rowslice, ]$x, dat.df[rowslice,]$x)
        #as.character due to data.frame's default stringsAsFactors = TRUE
        expect_equal (as.character(dat[rowslice, ]$animal), dat.df[rowslice,]$animal)
        expect_equal (dat[rowslice, ]$location, dat.df[rowslice,]$location)
    }

    #both
    for (rowslice in list(1:50, 50:100, 1, 100, c(20, 40, 60, 80))) {
        for (colslice in list(c("x", "animal", "location"),
                              c("location", "x", "animal"))) {
            expect_true (all (dat[rowslice, colslice] ==
                                  dat.df[rowslice, colslice]))
        }
    }
})

test_that ("Multiplyr$envir() returns an environment", {
    env <- dat$envir()
    expect_true (is.environment(env))
    expect_true (all(c("x", "animal", "location") %in% ls(envir=env)))
    rm (env)
})

test_that ("with(dat$envir(), x) returns ...", {
    expect_equal (with (dat$envir(), x), dat.df$x)
    expect_equal (with (dat$envir(), animal), dat.df$animal)
    expect_equal (with (dat$envir(), location), dat.df$location)
    expect_equal (levels(with (dat$envir(), location)), levels(dat.df$location))

    expect_true (is.numeric(with (dat$envir(), x)))
    expect_false (is.numeric(with (dat$envir(), animal)))
    expect_false (is.numeric(with (dat$envir(), location)))

    expect_false (is.factor(with (dat$envir(), x)))
    expect_false (is.factor(with (dat$envir(), animal)))
    expect_true (is.factor(with (dat$envir(), location)))

    expect_false (is.character(with (dat$envir(), x)))
    expect_true (is.character(with (dat$envir(), animal)))
    expect_false (is.character(with (dat$envir(), location)))
})

test_that ("dat[\"x\"] <- ... sets x to ...", {
    for (i in list(123, 1:100, 100:1, rep(0, 100))) {
        dat["x"] <- i
        dat.df["x"] <- i
        expect_equal (dat["x"], dat.df$x)
        expect_true (is.numeric(dat["x"]))
        expect_false (is.factor(dat["x"]))
    }
    for (i in list(rep(c("cow", "dog", "cat", "mouse"), each=25),
                   rep(c("cow", "dog"), each=50),
                   rep(c("dog", "cat", "mouse", "cow"), length.out=100),
                   "dog"
    )) {
        dat["animal"] <- i
        dat.df["animal"] <- i
        expect_equal (dat["animal"], dat.df$animal)
        expect_true (is.character(dat["animal"]))
    }
    for (i in list(rep(c("space", "house", "field"), length.out=100),
                   rep(c("house", "space"), each=50),
                   "field"
    )) {
        dat["location"] <- i
        dat.df["location"] <- i
        #Quirk: data.frame makes a character vector, Multiplyr still a factor
        expect_equal (as.character(dat["location"]), as.character(dat.df$location))
        expect_true (is.factor(dat["location"]))
    }
})

test_that ("data persists in Multiplyr$envir()", {
    dat["x"] <- 0
    with (dat$envir(), {i <- 123})
    with (dat$envir(), {x <- i})
    expect_equal (dat["x"], rep(123, 100))
})

test_that ("with(dat$envir(), x <- ...) sets x to ...", {
    with(dat$envir(), x <- 100:1)
    dat.df["x"] <- 100:1
    #deliberately different get method to set method
    expect_equal (dat["x"], dat.df$x)
    expect_true (is.numeric(dat["x"]))
    expect_false (is.factor(dat["x"]))
    with(dat$envir(), x <- 1:100)
    dat.df["x"] <- 1:100
    expect_equal (dat["x"], dat.df$x)

    with (dat$envir(), animal <- rep(c("cow", "dog", "cat", "mouse"), each=25))
    dat.df["animal"] <- rep(c("cow", "dog", "cat", "mouse"), each=25)
    expect_equal (dat["animal"], dat.df$animal)
    expect_true (is.character(dat["animal"]))
    with (dat$envir(), animal <- rep(c("dog", "mouse", "cow", "cat"), each=25))
    dat.df["animal"] <- rep(c("dog", "mouse", "cow", "cat"), each=25)
    expect_equal (dat["animal"], dat.df$animal)

    with (dat$envir(), location <- rep(c("space", "house", "field"), length.out=100))
    dat.df["location"] <- rep(c("space", "house", "field"), length.out=100)
    #Quirk: data.frame makes a character vector, Multiplyr still a factor
    expect_equal (as.character(dat["location"]), as.character(dat.df$location))
    expect_true (is.factor(dat["location"]))
    with (dat$envir(), location <- rep(c("field", "space", "house"), length.out=100))
    dat.df["location"] <- rep(c("field", "space", "house"), length.out=100)
    expect_equal (as.character(dat["location"]), as.character(dat.df$location))
})

#FIXME: row/col replacement
# test_that ("Multiplyr[x, y] <- works as expected", {
#     for (i in list(1:100, 100:1, 1:10, 50:60, 1, 100, c(20, 40, 60))) {
#         dat[, "x"] <- dat.df[, "x"] <- 0
#         dat[i, "x"] <- dat.df[i, "x"] <- 123
#         expect_equal (dat$x, dat.df$x)
#     }
#     for (i in list(1:100, 100:1, 1:10, 50:60, 1, 100, c(20, 40, 60))) {
#         dat[, "animal"] <- dat.df[, "animal"] <- "cow"
#         dat[i, "animal"] <- dat.df[i, "animal"] <- "dog"
#         expect_equal (dat$animal, dat.df$animal)
#     }
#     for (i in list(1:100, 100:1, 1:10, 50:60, 1, 100, c(20, 40, 60))) {
#         dat[, "location"] <- dat.df[, "location"] <- "house"
#         dat[i, "location"] <- dat.df[i, "location"] <- "field"
#         expect_equal (as.character(dat$location), as.character(dat.df$location))
#     }
# })

test_that ("$alloc_col() can allocate new columns/throw errors if no space", {
    dat <- Multiplyr(x=1:100, y=100:1, alloc=3, cl=cl2)

    dat$alloc_col ("a")
    expect_true ("a" %in% dat$col.names)

    dat$alloc_col (c("b", "c"))
    expect_true (all(c("b", "c") %in% dat$col.names))

    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)

    expect_error (dat$alloc_col ("d"), "free columns")

    rm (dat)
})

test_that ("$alloc_col() can return a mix of new and existing columns", {
    dat <- Multiplyr(x=1:100, y=100:1, alloc=3, cl=cl2)

    expect_equal (sum(is.na(dat$col.names)), 3)
    expect_equal (dat$alloc_col("x"), 1)
    expect_equal (dat$alloc_col("y"), 2)
    expect_equal (sum(is.na(dat$col.names)), 3)

    res <- dat$alloc_col(c("x", "a", "y", "b"))
    expect_equal (res, c(1, 3, 2, 4))
    expect_true (all(c("x", "y", "a", "b") %in% dat$col.names))

    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)

    expect_error (dat$alloc_col (c("b", "c", "d")), "free columns")

    rm (dat)
})
test_that ("$alloc_col(update=...) works appropriately", {
    dat <- Multiplyr(x=1:100, G=rep(c("A", "B"), each=50), alloc=3, cl=cl2)
    dat %>% group_by (G)
    dat$alloc_col ("y")
    expect_false ("y" %in% do.call (c, dat$cluster_eval (.local$col.names)))
    expect_false ("y" %in% do.call (c, dat$cluster_eval (.grouped[[1]]$col.names)))
    dat$alloc_col ("z", update=TRUE)
    expect_true ("z" %in% do.call (c, dat$cluster_eval (.local$col.names)))
    expect_true ("z" %in% do.call (c, dat$cluster_eval (.grouped[[1]]$col.names)))
    rm (dat)
})


test_that ("$calc_group_sizes() works appropriately", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat %>% group_by (G)

    dat$calc_group_sizes (delay=FALSE)
    expect_equal (dat$group_sizes, rep(25, 4))

    dat$bm[, dat$filtercol] <- rep(c(rep(1, 10), rep(0, 15)), 4)
    dat$filtered <- TRUE

    #delay=TRUE should set stale, but not change group sizes
    dat$group_sizes_stale <- FALSE
    dat$calc_group_sizes(delay=TRUE)
    expect_true (dat$group_sizes_stale)
    expect_equal (dat$group_sizes, rep(25, 4))

    #delay=TRUE should not change group sizes even when stale
    dat$group_sizes_stale <- TRUE
    dat$calc_group_sizes(delay=TRUE)
    expect_equal (dat$group_sizes, rep(25, 4))

    #delay=FALSE should not change sizes if not stale
    dat$group_sizes_stale <- FALSE
    dat$calc_group_sizes(delay=FALSE)
    expect_equal (dat$group_sizes, rep(25, 4))

    #delay=FALSE with stale=TRUE should update
    dat$group_sizes_stale <- TRUE
    dat$calc_group_sizes(delay=FALSE)
    expect_equal (dat$group_sizes, rep(10, 4))

    dat$empty <- TRUE
    dat$group_sizes_stale <- TRUE
    dat$calc_group_sizes (delay=FALSE)
    expect_equal (dat$group_sizes, rep(0, 4))

    rm (dat)
})

test_that ("$cluster_eval(...) evaluates ...", {
    dat <- Multiplyr(x=1:100, cl=cl2)
    expect_equal (do.call(c, dat$cluster_eval(123)), c(123, 123))
    rm (dat)
})

test_that ("$cluster_export() works as expected", {
    dat <- Multiplyr(x=1:100, cl=cl2)

    y <- 123
    z <- 456

    dat$cluster_export ("y")
    expect_equal (do.call (c, dat$cluster_eval(y)), c(123, 123))

    dat$cluster_export ("y", ".y")
    expect_equal (do.call (c, dat$cluster_eval(.y)), c(123, 123))

    dat$cluster_export (c("y", "z"), c("a", "b"))
    expect_equal (do.call (c, dat$cluster_eval(a)), c(123, 123))
    expect_equal (do.call (c, dat$cluster_eval(b)), c(456, 456))

    expect_error (dat$cluster_export (c("y", "z"), "q"), "same length")

    rm (dat)
})

test_that ("$cluster_export_each(...) exports individual cells of ... to each node", {
    dat <- Multiplyr (x=1:100, cl=cl2)

    y <- c(123, 456)
    z <- c(111, 222)
    dat$cluster_export_each ("y")
    expect_equal (dat$cluster_eval(y), list(123, 456))
    expect_equal (dat$cluster_eval(length(y)), list(1, 1))

    dat$cluster_export_each ("z", ".z")
    expect_equal (dat$cluster_eval(.z), list(111, 222))
    expect_equal (dat$cluster_eval(length(.z)), list(1, 1))

    dat$cluster_export_each (c("y", "z"), c("a", "b"))
    expect_equal (dat$cluster_eval(a), list(123, 456))
    expect_equal (dat$cluster_eval(b), list(111, 222))
    expect_equal (dat$cluster_eval(length(a)), list(1, 1))
    expect_equal (dat$cluster_eval(length(b)), list(1, 1))

    expect_error (dat$cluster_export (c("y", "z"), "c"), "same length")

    rm (dat)
})

test_that ("$copy() works as expected", {
    dat <- Multiplyr (x=1:100, cl=cl2)
    expect_error (dat$copy(shallow=FALSE), "safely")
    cpy <- dat$copy(shallow=TRUE)
    skip.fields <- c("profile_names",
                     "profile_user", "profile_sys", "profile_real",
                     "profile_ruser", "profile_rsys", "profile_rreal")
    fields <- ls(dat$.refClassDef@fieldPrototypes)
    fields <- fields[!fields %in% skip.fields]
    for (f in fields) {
        expect_equal (dat$field(f), cpy$field(f), info=f)
    }
})

test_that ("$describe() returns the appropriate structure", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    dat %>% group_by (G)

    res <- dat$describe()
    skip.fields <- c("bm", "bm.master", "cls", "bindenv")
    fields <- ls(dat$.refClassDef@fieldPrototypes)
    fields <- fields[!fields %in% skip.fields]

    expect_true ("Multiplyr.desc" %in% class(res))
    expect_equal (sort(names(res)), sort(fields))
    for (i in 1:length(fields)) {
        nm <- names(res)[i]
        expect_equal (res[[i]], dat$field(nm), info=nm)
    }

    rm (dat)
})

test_that ("$destroy_grouped() destroys .grouped on cluster", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    dat %>% group_by (G)

    expect_equal (dat$cluster_eval(length(.grouped)), list(1, 1))
    dat$destroy_grouped()
    expect_equal (dat$cluster_eval(exists(".grouped")), list(FALSE, FALSE))
    rm (dat)
})

test_that ("$factor_map() works appropriately", {
    dat <- Multiplyr (F=as.factor(rep(c("A", "B", "C", "D"), each=25)),
                      G=as.factor(rep(c("C", "D"), each=50)), cl=cl2)
    expect_equal (dat$factor_map ("F", c("D", "D", "A", "C", "B")),
                  c(4, 4, 1, 3, 2))
    expect_equal (dat$factor_map ("G", c("D", "D", "C", "D")),
                  c(2, 2, 1, 2))
    rm (dat)
})

test_that ("$filter_range() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)

    #1:50
    dat$filter_range (1, 50)
    expect_equal (dat$bm[, dat$filtercol], rep(c(1, 0), each=50))
    expect_equal (dat["x"], 1:50)

    #1:10 (+ 1:50 effectively)
    dat$filter_range (1, 10)
    expect_equal (dat$bm[, dat$filtercol], c(rep(1, 10), rep(0, 90)))
    expect_equal (dat["x"], 1:10)

    #reset
    dat$bm[, dat$filtercol] <- 1
    dat$filtered <- FALSE

    #11:20
    dat$filter_range (11, 20)
    expect_equal (dat$bm[, dat$filtercol], c(
        rep(0, 10),
        rep(1, 10),
        rep(0, 80)
        ))
    expect_equal (dat["x"], 11:20)

    #reset
    dat$bm[, dat$filtercol] <- 1
    dat$filtered <- FALSE

    dat$filter_range (51, 100)
    expect_equal (dat$bm[, dat$filtercol], rep(c(0, 1), each=50))
    expect_equal (dat["x"], 51:100)

    rm (dat)
})
test_that ("$filter_rows() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)

    dat$filter_rows (1:50)
    expect_equal (dat["x"], 1:50)

    dat$filter_rows (41:50)
    expect_equal (dat["x"], 41:50)

    rm (dat)
})
test_that ("$filter_vector() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)

    dat$filter_vector (rep(c(TRUE, FALSE), each=50))
    expect_equal (dat["x"], 1:50)

    dat$filter_vector (rep(c(TRUE, FALSE), length.out=100))
    expect_equal (dat["x"], (1:50)[rep(c(TRUE, FALSE), length.out=50)])

    rm (dat)
})

test_that ("$free_col() drops columns correctly", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat %>% group_by (G)
    expect_error (dat$free_col(dat$groupcol), "Attempted")
    expect_error (dat$free_col(dat$tmpcol), "Attempted")
    expect_error (dat$free_col(dat$filtercol), "Attempted")
    expect_error (dat$free_col(3), "Attempted")

    expect_equal (names(dat), c("x", "f", "G"))
    dat$free_col (1)
    expect_equal (names(dat), c("f", "G"))

    expect_equal (dat$factor.cols, c(2, 3))
    expect_equal (dat$factor.levels[[1]], c("f1", "f2"))
    expect_equal (dat$factor.levels[[2]], c("A", "B", "C", "D"))
    dat$free_col (2)
    expect_equal (dat$factor.cols, 3)
    expect_equal (dat$factor.levels[[1]], c("A", "B", "C", "D"))

    rm (dat)
})

dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
test_that ("$get_data(NULL, NULL) returns entire data", {
    expect_equal (dat$get_data(NULL, NULL), data.frame(x=1:100, G=rep(c("A", "B"), each=50)))
})

test_that ("$get_data(i, NULL) returns a row slice", {
    expect_equal (dat$get_data(1:50, NULL), data.frame(x=1:50, G=rep("A", 50)))
    expect_equal (dat$get_data(51:100, NULL), data.frame(x=51:100, G=rep("B", 50)))
    v <- rep(c(TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, NULL), data.frame(x=(1:100)[v], G=rep(c("A", "B"), each=50)[v]))
    v <- rep(c(FALSE, TRUE, TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, NULL), data.frame(x=(1:100)[v], G=rep(c("A", "B"), each=50)[v]))
})

test_that ("$get_data(NULL, j) returns specified columns", {
    expect_equal (dat$get_data(NULL, "x"), 1:100)
    expect_equal (dat$get_data(NULL, "G"), rep(c("A", "B"), each=50))
    expect_equal (dat$get_data(NULL, c("x", "G")), data.frame(x=1:100, G=rep(c("A", "B"), each=50)))
})

test_that ("$get_data(i, j) returns row/column subset", {
    expect_equal (dat$get_data(1:50, "x"), 1:50)
    expect_equal (dat$get_data(51:100, "x"), 51:100)
    v <- rep(c(TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, "x"), (1:100)[v])
    v <- rep(c(FALSE, TRUE, TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, "x"), (1:100)[v])

    expect_equal (dat$get_data(1:50, "G"), rep("A", 50))
    expect_equal (dat$get_data(51:100, "G"), rep("B", 50))
    v <- rep(c(TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, "G"), rep(c("A", "B"), each=50)[v])
    v <- rep(c(FALSE, TRUE, TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, "G"), rep(c("A", "B"), each=50)[v])
})

test_that ("$get_data(drop=FALSE) works appropriately", {
    expect_equal (dat$get_data(NULL, "x", drop=FALSE), data.frame(x=1:100))
    expect_equivalent (dat$get_data(NULL, c("x", "G"), drop=FALSE), data.frame(x=1:100, G=rep(c("A", "B"), each=50)))
})

test_that ("$get_data() throws errors for invalid row/column references", {
    expect_error (dat$get_data (0, "x"))
    expect_error (dat$get_data (101, "x"))
    expect_error (dat$get_data (1, "y"))
    expect_error (dat$get_data (99:101, "x"))
    expect_error (dat$get_data (99:100, c("x", "y")))

    expect_error (dat$get_data (NULL, "y"))
    expect_error (dat$get_data (NULL, c("x", "y")))
    expect_error (dat$get_data (0, NULL))
    expect_error (dat$get_data (101, NULL))
    expect_error (dat$get_data (99:101, NULL))
    expect_error (dat$get_data (0:10, NULL))
})

test_that ("$get_data() works on filtered data", {
    v <- rep(c(TRUE, FALSE, FALSE, TRUE), length.out=100)
    dat$filter_vector (v)

    expect_equivalent (dat$get_data(NULL, NULL), data.frame(x=1:100, G=rep(c("A", "B"), each=50))[v, ])
    expect_equal (dat$get_data(NULL, "x"), (1:100)[v])
    expect_equal (dat$get_data(NULL, "G"), rep(c("A", "B"), each=50)[v])
    expect_equivalent (dat$get_data(NULL, c("x", "G")), data.frame(x=1:100, G=rep(c("A", "B"), each=50))[v, ])

    r <- 2:11
    rv <- v; rv[which(rv)[-r]] <- FALSE
    expect_equivalent (dat$get_data(r, NULL), data.frame(x=1:100, G=rep(c("A", "B"), each=50))[rv, ])
    expect_equal (dat$get_data(r, "x"), (1:100)[rv])
    expect_equal (dat$get_data(r, "G"), rep(c("A", "B"), each=50)[rv])
    expect_equivalent (dat$get_data(r, c("x", "G")), data.frame(x=1:100, G=rep(c("A", "B"), each=50))[rv, ])
})
rm (dat)

test_that ("$group_restrict(...) returns a group restricted data frame", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat %>% group_by (G)
    grp <- dat$group_restrict (3)
    expect_equal (grp["x"], 51:75)
    expect_equal (grp["G"], rep("C", 25))
    expect_equal (dat["x"], 1:100)
    expect_equal (dat["G"], rep(c("A", "B", "C", "D"), each=25))
    rm (dat)
})

test_that ("$group_restrict(...) throws an error for no parameters or non-grouped data", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    expect_error (dat$group_restrict(1), "grouped")
    dat %>% group_by (G)
    expect_error (dat$group_restrict(), "specify")
    rm (dat)
})

test_that ("$group_restrict() for an empty group returns a frame with $empty=TRUE ", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat %>% group_by (G)
    dat %>% filter(x<50)
    grp <- dat$group_restrict(3)
    expect_true (grp$empty)
    rm (dat)
})

parallel::stopCluster (cl2)
