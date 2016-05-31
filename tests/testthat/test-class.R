context("class")

cl2 <- parallel::makeCluster(2)

test_that ("Multiplyr(x=...) creates the appropriate structure", {
    x <- 1:100
    f.src <- factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2"))
    dat <- Multiplyr(x, f=f.src, G=rep(c("A", "B", "C", "D"), each=25), alloc=1, cl=cl2)

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

    expect_equal (dat$group.cols, numeric(0))
    expect_equal (dat$grouped, FALSE)
    expect_equal (dat$group, numeric(0))
    expect_equal (dat$group_partition, FALSE)
    expect_equal (dat$empty, FALSE)
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

    expect_equal (dat$group.cols, numeric(0))
    expect_equal (dat$grouped, FALSE)
    expect_equal (dat$group, numeric(0))
    expect_equal (dat$group_partition, FALSE)
    expect_equal (dat$empty, FALSE)
})

test_that ("Multiplyr's destructor calls stopCluster", {
    if (packageVersion("testthat") < "0.11.0") {
        skip ("Testing for non-warning requires testthat >= 0.11")
    }

    #Warning messages:
    #1: closing unused connection 5 (<-localhost:11064)
    dat <- Multiplyr (x=1:100, cl=1)
    dat <- Multiplyr (x=1:100, cl=cl2)
    expect_warning (gc(), regexp=NA)
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

    expect_equal (dat$group.cols, numeric(0))
    expect_equal (dat$grouped, FALSE)
    expect_equal (dat$group, numeric(0))
    expect_equal (dat$group_partition, FALSE)
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

rm (dat)
rm (dat.df)

test_that ("$alloc_col() can allocate new columns/throw errors if no space", {
    dat <- Multiplyr(x=1:100, y=100:1, alloc=3, cl=cl2)

    dat$alloc_col ("a")
    expect_true ("a" %in% dat$col.names)

    dat$alloc_col (c("b", "c"))
    expect_true (all(c("b", "c") %in% dat$col.names))

    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)

    expect_error (dat$alloc_col ("d"), "free columns")
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
})
test_that ("$alloc_col(update=...) works appropriately", {
    dat <- Multiplyr(x=1:100, G=rep(c("A", "B"), each=50), alloc=3, cl=cl2)
    dat %>% group_by (G)
    dat$alloc_col ("y")
    expect_false ("y" %in% do.call (c, dat$cluster_eval (.local$col.names)))
    dat$alloc_col ("z", update=TRUE)
    expect_true ("z" %in% do.call (c, dat$cluster_eval (.local$col.names)))
})

#FIXME: build_grouped

test_that ("$calc_group_sizes() works appropriately", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat %>% group_by (G)

    dat$calc_group_sizes (delay=FALSE)
    expect_equal (dat$group_cache[, 3], rep(25, 4))

    dat$bm[, dat$filtercol] <- rep(c(rep(1, 10), rep(0, 15)), 4)
    dat$filtered <- TRUE

    #delay=TRUE should set stale, but not change group sizes
    dat$group_sizes_stale <- FALSE
    dat$calc_group_sizes(delay=TRUE)
    expect_true (dat$group_sizes_stale)
    expect_equal (dat$group_cache[, 3], rep(25, 4))

    #delay=TRUE should not change group sizes even when stale
    dat$group_sizes_stale <- TRUE
    dat$calc_group_sizes(delay=TRUE)
    expect_equal (dat$group_cache[, 3], rep(25, 4))

    #delay=FALSE should not change sizes if not stale
    dat$group_sizes_stale <- FALSE
    dat$calc_group_sizes(delay=FALSE)
    expect_equal (dat$group_cache[, 3], rep(25, 4))

    #delay=FALSE with stale=TRUE should update
    dat$group_sizes_stale <- TRUE
    dat$calc_group_sizes(delay=FALSE)
    expect_equal (group_sizes(dat), rep(10, 4))

    dat$empty <- TRUE
    dat$group_sizes_stale <- TRUE
    dat$calc_group_sizes (delay=FALSE)
    expect_equal (group_sizes(dat), rep(0, 4))
})

test_that ("$cluster_eval(...) evaluates ...", {
    dat <- Multiplyr(x=1:100, cl=cl2)
    expect_equal (do.call(c, dat$cluster_eval(123)), c(123, 123))
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
    skip.fields <- c("bm", "bm.master", "group_cache", "cls", "bindenv", "groupenv", "savestate")
    fields <- ls(dat$.refClassDef@fieldPrototypes)
    fields <- fields[!fields %in% skip.fields]

    expect_true ("Multiplyr.desc" %in% class(res))
    expect_equal (sort(names(res)), sort(fields))
    for (i in 1:length(fields)) {
        nm <- names(res)[i]
        expect_equal (res[[i]], dat$field(nm), info=nm)
    }
})

test_that ("$factor_map() works appropriately", {
    dat <- Multiplyr (F=as.factor(rep(c("A", "B", "C", "D"), each=25)),
                      G=as.factor(rep(c("C", "D"), each=50)), cl=cl2)
    expect_equal (dat$factor_map ("F", c("D", "D", "A", "C", "B")),
                  c(4, 4, 1, 3, 2))
    expect_equal (dat$factor_map ("G", c("D", "D", "C", "D", "C")),
                  c(2, 2, 1, 2, 1))
    expect_equal (dat$factor_map (c("G", "F"), data.frame(G=c("D", "D", "C", "D", "C"), F=c("D", "D", "A", "C", "B"), stringsAsFactors=FALSE)),
                  matrix (c(c(2, 2, 1, 2, 1), c(4, 4, 1, 3, 2)), ncol=2))
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
})
test_that ("$filter_rows() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)

    dat$filter_rows (1:50)
    expect_equal (dat["x"], 1:50)

    dat$filter_rows (41:50)
    expect_equal (dat["x"], 41:50)
})
test_that ("$filter_vector() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)

    dat$filter_vector (rep(c(TRUE, FALSE), each=50))
    expect_equal (dat["x"], 1:50)

    dat$filter_vector (rep(c(TRUE, FALSE), length.out=100))
    expect_equal (dat["x"], (1:50)[rep(c(TRUE, FALSE), length.out=50)])
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
})

dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
test_that ("$get_data(NULL, NULL) returns entire data", {
    expect_equal (dat$get_data(NULL, NULL), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE))
})

test_that ("$get_data(i, NULL) returns a row slice", {
    expect_equal (dat$get_data(1:50, NULL), data.frame(x=1:50, G=rep("A", 50), stringsAsFactors = FALSE))
    expect_equal (dat$get_data(51:100, NULL), data.frame(x=51:100, G=rep("B", 50), stringsAsFactors = FALSE))
    v <- rep(c(TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, NULL), data.frame(x=(1:100)[v], G=rep(c("A", "B"), each=50)[v], stringsAsFactors = FALSE))
    v <- rep(c(FALSE, TRUE, TRUE, FALSE), length.out=100)
    expect_equal (dat$get_data(v, NULL), data.frame(x=(1:100)[v], G=rep(c("A", "B"), each=50)[v], stringsAsFactors = FALSE))
})

test_that ("$get_data(NULL, j) returns specified columns", {
    expect_equal (dat$get_data(NULL, "x"), 1:100)
    expect_equal (dat$get_data(NULL, "G"), rep(c("A", "B"), each=50))
    expect_equal (dat$get_data(NULL, c("x", "G")), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE))
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
    expect_equivalent (dat$get_data(NULL, c("x", "G"), drop=FALSE), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE))
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

    expect_equivalent (dat$get_data(NULL, NULL), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE)[v, ])
    expect_equal (dat$get_data(NULL, "x"), (1:100)[v])
    expect_equal (dat$get_data(NULL, "G"), rep(c("A", "B"), each=50)[v])
    expect_equivalent (dat$get_data(NULL, c("x", "G")), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE)[v, ])

    r <- 2:11
    rv <- v; rv[which(rv)[-r]] <- FALSE
    expect_equivalent (dat$get_data(r, NULL), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE)[rv, ])
    expect_equal (dat$get_data(r, "x"), (1:100)[rv])
    expect_equal (dat$get_data(r, "G"), rep(c("A", "B"), each=50)[rv])
    expect_equivalent (dat$get_data(r, c("x", "G")), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE)[rv, ])
})
rm (dat)

test_that ("$group_restrict(...) sets/unsets data frame to group restricted mode", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat %>% group_by (G)
    dat$group_restrict (3)
    expect_equal (dat["x"], 51:75)
    expect_equal (dat["G"], rep("C", 25))
    dat$group_restrict ()
    expect_equal (dat["x"], 1:100)
    expect_equal (dat["G"], rep(c("A", "B", "C", "D"), each=25))
})

test_that ("$group_restrict(...) throws an error for non-grouped data", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    expect_error (dat$group_restrict(1), "grouped")
    dat %>% group_by (G)
})

test_that ("$group_restrict() for an empty group throws an error ", {
    dat <- Multiplyr(x=1:100,
                     f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                     G=rep(c("A", "B", "C", "D"), each=25),
                     alloc=1,
                     cl=cl2)
    dat %>% group_by (G)
    dat %>% filter(x<50)
    dat$group_restrict(3)
    expect_true (dat$empty)
    dat$group_restrict()
    expect_false (dat$empty)
})

test_that ("$local_subset() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    dat$local_subset (50, 60)
    expect_equal (dat$first, 50)
    expect_equal (dat$last, 60)
    expect_equal (dat["x"], 50:60)
})

#$partition_even -> test_partition.R

#FIXME: profile, profile_import
#FIXME: reattach_slave
#FIXME: rebuild_grouped

test_that ("$row_names() works appropriately", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    expect_equal (dat$row_names(), 1:100)
    dat$filter_range (51, 60)
    expect_equal (dat$row_names(), 1:10)
    dat$filter_vector (rep(FALSE, 100))
    expect_equal (dat$row_names(), character(0))
})

test_that ("$set_data(NULL, NULL) sets entire dataset", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    dat$set_data(NULL, NULL, data.frame (x=100:1, G=rep(c("A", "B"), length.out=100), stringsAsFactors = FALSE))
    expect_equivalent (dat$get_data(NULL, NULL), data.frame (x=100:1, G=rep(c("A", "B"), length.out=100), stringsAsFactors = FALSE))
})

test_that ("$set_data(i, NULL) sets a row slice", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    dat$set_data (1:10, NULL, data.frame(x=10:1, G=rep("B", 10)))
    expect_equal (dat["x"], c(10:1, 11:100))
    expect_equal (dat["G"], c(rep("B", 10), rep("A", 40), rep("B", 50)))
})
test_that ("$set_data(NULL, j) sets specified columns", {
    dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
    dat$set_data (NULL, "x", 100:1)
    expect_equal (dat["x"], 100:1)
    expect_equal (dat["G"], rep(c("A", "B"), each=50))
})
test_that ("$set_data(i, j) sets row/column subset", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), cl=cl2)
    dat$set_data (1:10, "x", 10:1)
    expect_equal (dat["x"], c(10:1, 11:100))
    expect_equal (dat["y"], 100:1)
    expect_equal (dat["G"], rep(c("A", "B"), each=50))
    dat$set_data (11:20, c("x", "y"), matrix(c(10:1, 1:10), ncol=2))
    expect_equal (dat["x"], c(10:1, 10:1, 21:100))
    expect_equal (dat["y"], c(100:91, 1:10, 80:1))
    expect_equal (dat["G"], rep(c("A", "B"), each=50))
})
test_that ("$set_data() works on filtered data", {
    v <- rep(c(TRUE, FALSE, FALSE, TRUE), length.out=100)
    dat.df <- data.frame (x=1:100, y=100:1, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE)
    dat <- Multiplyr (dat.df, cl=cl2)
    dat$filter_vector (v)

    r <- 2:11
    rv <- v; rv[which(rv)[-r]] <- FALSE
    dat$set_data (r, c("x", "G"), data.frame(x=10:1, G=rep("B", 10), stringsAsFactors = FALSE))
    dat.df[rv, "x"] <- 10:1
    dat.df[rv, "G"] <- rep("B", 10)
    expect_equal (dat["x"], dat.df[v, "x"])
    expect_equal (dat["y"], dat.df[v, "y"])
    expect_equal (dat["G"], dat.df[v, "G"])
})

dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
test_that ("$set_data() throws errors for invalid row/column references", {
    expect_error (dat$set_data (NULL, "y", 1:100), "Undefined")
    expect_error (dat$set_data (NULL, 10, 1:100), "Invalid")
    expect_error (dat$set_data (NULL, 0, 1:100), "Invalid")
    expect_error (dat$set_data (NULL, c("x", "y"), data.frame(x=1:100, y=1:100)), "Undefined")
    expect_error (dat$set_data (NULL, c(1, 0), matrix(1, nrow=100, ncol=2)), "Invalid")

    expect_error (dat$set_data (0, NULL, data.frame(x=1, G="A", stringsAsFactors=FALSE)), "Invalid")
    expect_error (dat$set_data (101, NULL, data.frame(x=1, G="A", stringsAsFactors=FALSE)), "Invalid")
    expect_error (dat$set_data (99:101, NULL, data.frame(x=1:3, G=rep("A", 3), stringsAsFactors=FALSE)), "Invalid")
    expect_error (dat$set_data (99:101, "x", 1:10), "Invalid")
    expect_error (dat$set_data (1:10, "y", 1:10), "Undefined")
})
test_that ("$set_data() throws error for mismatched lengths", {
    expect_error (dat$set_data (NULL, "x", 1:101), "replacement")
    expect_error (dat$set_data (NULL, "x", 1:99), "replacement")
    expect_error (dat$set_data (1:10, "x", 1:11), "replacement")
    expect_error (dat$set_data (1:10, "x", 1:9), "replacement")

    expect_error (dat$set_data (NULL, c("x", "G"), 1:100), "replacement")
    expect_error (dat$set_data (NULL, c("x", "G"), data.frame(x=1:101, G=rep("A", 101), stringsAsFactors=FALSE)), "replacement")
})
rm (dat)

#FIXME: show

test_that ("$sort() returns sorted data", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), length.out=100), cl=cl2)

    dat$sort (decreasing=FALSE, cols=2)
    expect_equal (dat["x"], 100:1)
    expect_equal (dat["y"], 1:100)
    expect_equal (dat["G"], rep(c("B", "A"), length.out=100))

    dat$sort (decreasing=TRUE, cols=2)
    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)
    expect_equal (dat["G"], rep(c("A", "B"), length.out=100))

    dat$sort (decreasing=TRUE, cols=c(1,2))
    expect_equal (dat["x"], 100:1)
    expect_equal (dat["y"], 1:100)
    expect_equal (dat["G"], rep(c("B", "A"), length.out=100))

    dat %>% group_by (G)
    dat$sort (decreasing=FALSE, cols=1)
    expect_equal (dat["x"], c(seq(1, 100, by=2), seq(2, 100, by=2)))
    expect_equal (dat["G"], rep(c("A", "B"), each=50))
    dat$sort (decreasing=FALSE, cols=1, with.group=FALSE)
    expect_equal (dat["x"], 1:100)
    expect_equal (dat["y"], 100:1)
    expect_equal (dat["G"], rep(c("A", "B"), length.out=100))
    dat %>% ungroup()

    f <- function (...) { dotscapture (...) }

    .dots <- f(y)
    dat$sort (decreasing=FALSE, dots=.dots)
    expect_equal (dat["x"], 100:1)
    expect_equal (dat["y"], 1:100)
    expect_equal (dat["G"], rep(c("B", "A"), length.out=100))

    .dots <- f(G, x)
    dat$sort (decreasing=FALSE, dots=.dots)
    expect_equal (dat["x"], c(seq(1, 100, by=2), seq(2, 100, by=2)))
    expect_equal (dat["G"], rep(c("A", "B"), each=50))

    rm (f)
    rm (.dots)
})

test_that ("$update_fields() works appropriately", {
    dat <- Multiplyr (x=1:100, y=100:1, G=rep(c("A", "B"), length.out=100), cl=cl2)
    dat %>% group_by (G)
    dat$pad <- 1:100
    dat$update_fields ("pad")
    expect_equal (dat$cluster_eval(.local$pad), list(1:100, 1:100))
})

dat <- Multiplyr (x=1:100, G=rep(c("A", "B"), each=50), cl=cl2)
test_that ("as.data.frame() works appropriately", {
    expect_equal (as.data.frame (dat), data.frame(x=1:100, G=rep(c("A", "B"), each=50), stringsAsFactors = FALSE))
})

test_that ("dimnames() works appropriately", {
    expect_equal (dimnames(dat), list(row.names(dat), names(dat)))
})

test_that ("names() works appropriately", {
    expect_equal (names(dat), c("x", "G"))
    dat %>% select (G, x)
    expect_equal (names(dat), c("G", "x"))
})

test_that ("row.names() works appropriately", {
    expect_equal (row.names(dat), 1:100)
    dat$filter_range (51, 60)
    expect_equal (row.names(dat), 1:10)
    dat$filter_vector (rep(FALSE, 100))
    expect_equal (row.names(dat), character(0))
})
rm (dat)

#Attempt to stop "no function to return from, jumping to top level"
gc()

parallel::stopCluster (cl2)
