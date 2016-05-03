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

    expect_equal (dat$col.names[1:3], c("x", "f", "G"))
    expect_equal (dat$order.cols[1:3], c(1, 2, 3))
    expect_equal (dat$type.cols[1:3], c(0, 1, 2))

    expect_equal (sum (dat$order.cols[-(1:3)]), 0)
    expect_true (all(c(".tmp", ".filter", ".group") %in% dat$col.names))

    expect_equal (dat$factor.cols, c(2, 3))
    expect_equal (dat$factor.levels[[1]], c("f1", "f2"))
    expect_equal (dat$factor.levels[[2]], c("A", "B", "C", "D"))

    expect_gte (sum(is.na(dat$col.names)), 1)

    expect_equal (length(dat$cls), 2)
    expect_true (all(c("SOCKcluster", "cluster") %in% class(dat$cls)))

    expect_equal (dat$grouped, FALSE)
    expect_equal (dat$group, 0)
    expect_equal (dat$group_partition, FALSE)

    rm (dat)
})
# .filter, .group, alloc

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

    expect_equal (dat$col.names[1:3], c("x", "f", "G"))
    expect_equal (dat$order.cols[1:3], c(1, 2, 3))
    expect_equal (dat$type.cols[1:3], c(0, 1, 2))

    expect_equal (sum (dat$order.cols[-(1:3)]), 0)
    expect_true (all(c(".tmp", ".filter", ".group") %in% dat$col.names))

    expect_equal (dat$factor.cols, c(2, 3))
    expect_equal (dat$factor.levels[[1]], c("f1", "f2"))
    expect_equal (dat$factor.levels[[2]], c("A", "B", "C", "D"))

    expect_gte (sum(is.na(dat$col.names)), 1)

    expect_equal (length(dat$cls), 2)
    expect_true (all(c("SOCKcluster", "cluster") %in% class(dat$cls)))

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

### Getting

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

### Setting

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
    with (dat, location <- rep(c("field", "space", "house"), length.out=100))
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

parallel::stopCluster (cl2)
