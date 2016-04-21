context("fastdf")

test_that ("fastdf(x=..., y=...) creates the appropriate structure", {
    dat <- fastdf(x=1:100,
                  f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                  G=rep(c("A", "B", "C", "D"), each=25),
                  alloc=1,
                  cl=2)

    expect_equal (nrow(dat[[1]]), 100)
    expect_gte (ncol(dat[[1]]), 3)
    expect_equal (class(dat), c("fastdf", "list"))

    expect_equal (attr(dat, "colnames")[1:3], c("x", "f", "G"))
    expect_equal (attr(dat, "order.cols")[1:3], c(1, 2, 3))
    expect_equal (attr(dat, "type.cols")[1:3], c(0, 1, 2))

    expect_equal (sum (attr(dat, "order.cols")[-(1:3)]), 0)
    expect_true (all(c(".filter", ".group") %in% attr(dat, "colnames")))

    expect_equal (attr(dat, "factor.cols"), c(2, 3))
    expect_equal (attr(dat, "factor.levels")[[1]], c("f1", "f2"))
    expect_equal (attr(dat, "factor.levels")[[2]], c("A", "B", "C", "D"))

    expect_gte (sum(is.na(attr(dat, "colnames"))), 1)

    expect_equal (length(attr(dat, "cl")), 2)
    expect_true (all(c("SOCKcluster", "cluster") %in% class(attr(dat, "cl"))))

    expect_equal (attr(dat, "grouped"), FALSE)
    expect_equal (attr(dat, "group"), 0)
    expect_equal (attr(dat, "group_partition"), FALSE)

    stopCluster (attr(dat, "cl"))
    rm (dat)
})
# .filter, .group, alloc

test_that ("as.fastdf() works on a data.frame", {
    dat.df <- data.frame(x=1:100,
                         f=factor(rep(c("f1", "f2"), each=50), levels=c("f1", "f2")),
                         G=rep(c("A", "B", "C", "D"), each=25),
                         stringsAsFactors = FALSE)
    dat <- fastdf(dat.df,
                  alloc=1,
                  cl=2)

    expect_equal (nrow(dat[[1]]), 100)
    expect_gte (ncol(dat[[1]]), 3)
    expect_equal (class(dat), c("fastdf", "list"))

    expect_equal (attr(dat, "colnames")[1:3], c("x", "f", "G"))
    expect_equal (attr(dat, "order.cols")[1:3], c(1, 2, 3))
    expect_equal (attr(dat, "type.cols")[1:3], c(0, 1, 2))

    expect_equal (sum (attr(dat, "order.cols")[-(1:3)]), 0)
    expect_true (all(c(".filter", ".group") %in% attr(dat, "colnames")))

    expect_equal (attr(dat, "factor.cols"), c(2, 3))
    expect_equal (attr(dat, "factor.levels")[[1]], c("f1", "f2"))
    expect_equal (attr(dat, "factor.levels")[[2]], c("A", "B", "C", "D"))

    expect_gte (sum(is.na(attr(dat, "colnames"))), 1)

    expect_equal (length(attr(dat, "cl")), 2)
    expect_true (all(c("SOCKcluster", "cluster") %in% class(attr(dat, "cl"))))

    expect_equal (attr(dat, "grouped"), FALSE)
    expect_equal (attr(dat, "group"), 0)
    expect_equal (attr(dat, "group_partition"), FALSE)

    stopCluster (attr(dat, "cl"))
    rm (dat)
})

N<-100
dat.df <- data.frame (x=N:1,
             animal=rep(c("dog", "cat", "mouse", "cow"), each=N/4),
             location=as.factor(rep(c("house", "field", "space"), length.out=N)),
             stringsAsFactors = FALSE)
dat <- fastdf (dat.df, alloc=1, cl=2)

### Getting

test_that ("fastdf(x=...)$x returns ...", {
    expect_equal (dat$x, dat.df$x)
    expect_equal (dat$animal, dat.df$animal)
    expect_equal (dat$location, dat.df$location)
    expect_equal (levels(dat$location), levels(dat.df$location))

    expect_true (is.numeric(dat$x))
    expect_false (is.numeric(dat$animal))
    expect_false (is.numeric(dat$location))

    expect_false (is.factor(dat$x))
    expect_false (is.factor(dat$animal))
    expect_true (is.factor(dat$location))

    expect_false (is.character(dat$x))
    expect_true (is.character(dat$animal))
    expect_false (is.character(dat$location))
})
#numeric, factor, text

test_that ("fastdf(x=...)[\"x\"] returns ...", {
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
#numeric, factor, text

test_that ("fastdf[x, y] subsets rows and columns appropriately", {
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

test_that ("as.environment(fastdf(x=...)) returns an environment", {
    env <- as.environment (dat)
    expect_true (is.environment(env))
    expect_true (all(c("x", "animal", "location") %in% ls(envir=env)))
    rm (env)
})

test_that ("with(fastdf(x=...), x) returns ...", {
    expect_equal (with (dat, x), dat.df$x)
    expect_equal (with (dat, animal), dat.df$animal)
    expect_equal (with (dat, location), dat.df$location)
    expect_equal (levels(with (dat, location)), levels(dat.df$location))

    expect_true (is.numeric(with (dat, x)))
    expect_false (is.numeric(with (dat, animal)))
    expect_false (is.numeric(with (dat, location)))

    expect_false (is.factor(with (dat, x)))
    expect_false (is.factor(with (dat, animal)))
    expect_true (is.factor(with (dat, location)))

    expect_false (is.character(with (dat, x)))
    expect_true (is.character(with (dat, animal)))
    expect_false (is.character(with (dat, location)))
})

### Setting

test_that ("fastdf$x <- ... sets x to ...", {
    for (i in list(123, 1:100, 100:1, rep(0, 100))) {
        dat$x <- i
        dat.df$x <- i
        #deliberately different get method to set method
        expect_equal (dat["x"], dat.df$x)
        expect_true (is.numeric(dat["x"]))
        expect_false (is.factor(dat["x"]))
    }
    for (i in list(rep(c("cow", "dog", "cat", "mouse"), each=25),
                   rep(c("cow", "dog"), each=50),
                   rep(c("dog", "cat", "mouse", "cow"), length.out=100),
                   "dog"
                   )) {
        dat$animal <- i
        dat.df$animal <- i
        #deliberately different get method to set method
        expect_equal (dat["animal"], dat.df$animal)
        expect_true (is.character(dat["animal"]))
    }
    for (i in list(rep(c("space", "house", "field"), length.out=100),
                   rep(c("house", "space"), each=50),
                   "field"
    )) {
        dat$location <- i
        dat.df$location <- i
        #deliberately different get method to set method
        #Quirk: data.frame makes a character vector, fastdf still a factor
        expect_equal (as.character(dat["location"]), as.character(dat.df$location))
        expect_true (is.factor(dat["location"]))
    }
})

test_that ("fastdf[\"x\"] <- ... sets x to ...", {
    for (i in list(123, 1:100, 100:1, rep(0, 100))) {
        dat["x"] <- i
        dat.df["x"] <- i
        #deliberately different get method to set method
        expect_equal (dat$x, dat.df$x)
        expect_true (is.numeric(dat$x))
        expect_false (is.factor(dat$x))
    }
    for (i in list(rep(c("cow", "dog", "cat", "mouse"), each=25),
                   rep(c("cow", "dog"), each=50),
                   rep(c("dog", "cat", "mouse", "cow"), length.out=100),
                   "dog"
    )) {
        dat["animal"] <- i
        dat.df["animal"] <- i
        #deliberately different get method to set method
        expect_equal (dat$animal, dat.df$animal)
        expect_true (is.character(dat$animal))
    }
    for (i in list(rep(c("space", "house", "field"), length.out=100),
                   rep(c("house", "space"), each=50),
                   "field"
    )) {
        dat["location"] <- i
        dat.df["location"] <- i
        #deliberately different get method to set method
        #Quirk: data.frame makes a character vector, fastdf still a factor
        expect_equal (as.character(dat$location), as.character(dat.df$location))
        expect_true (is.factor(dat$location))
    }
})

attr(dat, "bindenv") <- as.environment(dat)

test_that ("data persists in a fastdf bindenv", {
    dat$x <- 0
    with (dat, {i <- 123})
    with (dat, {x <- i})
})

test_that ("with(fastdf(), x <- ...) sets x to ...", {
    with(dat, x <- 100:1)
    dat.df["x"] <- 100:1
    #deliberately different get method to set method
    expect_equal (dat$x, dat.df$x)
    expect_true (is.numeric(dat$x))
    expect_false (is.factor(dat$x))
    with(dat, x <- 1:100)
    dat.df["x"] <- 1:100
    expect_equal (dat$x, dat.df$x)

    with (dat, animal <- rep(c("cow", "dog", "cat", "mouse"), each=25))
    dat.df["animal"] <- rep(c("cow", "dog", "cat", "mouse"), each=25)
    expect_equal (dat$animal, dat.df$animal)
    expect_true (is.character(dat$animal))
    with (dat, animal <- rep(c("dog", "mouse", "cow", "cat"), each=25))
    dat.df["animal"] <- rep(c("dog", "mouse", "cow", "cat"), each=25)
    expect_equal (dat$animal, dat.df$animal)

    with (dat, location <- rep(c("space", "house", "field"), length.out=100))
    dat.df["location"] <- rep(c("space", "house", "field"), length.out=100)
    #Quirk: data.frame makes a character vector, fastdf still a factor
    expect_equal (as.character(dat$location), as.character(dat.df$location))
    expect_true (is.factor(dat$location))
    with (dat, location <- rep(c("field", "space", "house"), length.out=100))
    dat.df["location"] <- rep(c("field", "space", "house"), length.out=100)
    expect_equal (as.character(dat$location), as.character(dat.df$location))
})

#FIXME: row/col replacement
# test_that ("fastdf[x, y] <- works as expected", {
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
