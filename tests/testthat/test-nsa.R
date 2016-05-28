context("nsa")

cl2 <- parallel::makeCluster (2)

test_that ("nsa() can set/unset NSA mode and throw errors", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)
    expect_equal (dat$nsamode, FALSE)
    dat %>% nsa(TRUE)
    expect_equal (dat$nsamode, TRUE)
    dat %>% nsa(FALSE)
    expect_equal (dat$nsamode, FALSE)
    dat %>% nsa()
    expect_equal (dat$nsamode, TRUE)
    expect_warning (dat %>% nsa(), "already in")
    expect_warning (dat %>% nsa(FALSE) %>% nsa(FALSE), "already not in")
    expect_error (data.frame(x=1:100) %>% nsa(), "Multiplyr")
    rm (dat)
})

test_that ("$get_data() can work in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    expect_equal (dat$get_data (NULL, NULL, nsa=TRUE),
                  matrix(c(rep(1:4, each=25), rep(1:2, length.out=100)), ncol=2))
    expect_equal (dat$get_data (NULL, c("P", "A"), nsa=TRUE),
                  matrix(c(rep(1:2, length.out=100), rep(1:4, each=25)), ncol=2))
    expect_equal (dat$get_data (NULL, "A", nsa=TRUE), rep(1:4, each=25))
    expect_equal (dat$get_data (NULL, "P", nsa=TRUE), rep(1:2, length.out=100))

    r <- 50:60
    expect_equal (dat$get_data (r, c("P", "A"), nsa=TRUE),
                  matrix(c(rep(1:2, length.out=100)[r], rep(1:4, each=25)[r]), ncol=2))
    expect_equal (dat$get_data (r, "A", nsa=TRUE), rep(1:4, each=25)[r])
    expect_equal (dat$get_data (r, "P", nsa=TRUE), rep(1:2, length.out=100)[r])

    r <- rep(c(TRUE, FALSE, FALSE, TRUE), length.out=100)
    expect_equal (dat$get_data (r, c("P", "A"), nsa=TRUE),
                  matrix(c(rep(1:2, length.out=100)[r], rep(1:4, each=25)[r]), ncol=2))
    expect_equal (dat$get_data (r, "A", nsa=TRUE), rep(1:4, each=25)[r])
    expect_equal (dat$get_data (r, "P", nsa=TRUE), rep(1:2, length.out=100)[r])

    rm (dat)
})
test_that ("$set_data() can work in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    dat$set_data (NULL, "A", rep(c(1,2), each=50), nsa=TRUE)
    expect_equal (dat["A"], rep(c("A", "B"), each=50))
    expect_equal (dat["P"], rep(c("p", "q"), length.out=100))

    rm (dat)
})

test_that ("filter() works in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    dat %>% nsa() %>% filter (A < 3) %>% nsa(FALSE)

    expect_equal (dat["A"], rep(c("A", "B"), each=25))
    expect_equal (dat["P"], rep(c("p", "q"), length.out=50))

    rm (dat)
})
test_that ("mutate() works in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    dat %>% nsa() %>% mutate (A=3, P=1) %>% nsa(FALSE)

    expect_equal (dat["A"], rep("C", 100))
    expect_equal (dat["P"], rep("p", 100))

    rm (dat)
})
test_that ("summarise() works in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    dat %>% group_by (A)
    dat %>% nsa() %>% summarise (N=(max(P)+min(P))) %>% nsa(FALSE)

    expect_equal (dat["A"], c("A", "B", "C", "D"))
    expect_equal (dat["N"], rep(3, 4))

    rm (dat)
})
test_that ("transmute() works in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    dat %>% nsa() %>% transmute (P=1) %>% nsa(FALSE)

    expect_equal (dat["P"], rep("p", 100))
    expect_false ("A" %in% dat$col.names)

    rm (dat)
})
test_that ("within_group() works in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    dat %>% nsa() %>% group_by(A) %>% within_group({
        if (A[1] < 3) {
            P <- rep(1, length(P))
        } else {
            P <- rep(2, length(P))
        }
    }) %>% nsa(FALSE)

    expect_equal (dat["A"], rep(c("A", "B", "C", "D"), each=25))
    expect_equal (dat["P"], rep(c("p", "q"), each=50))

    rm (dat)
})
test_that ("within_node() works in NSA-mode", {
    dat <- Multiplyr (A=rep(c("A", "B", "C", "D"), each=25), P=rep(c("p", "q"), length.out=100), cl=cl2)

    dat %>% nsa() %>% within_node({
        P <- 2
    }) %>% nsa(FALSE)

    expect_equal (dat["A"], rep(c("A", "B", "C", "D"), each=25))
    expect_equal (dat["P"], rep("q", 100))

    rm (dat)
})

#Attempt to stop "no function to return from, jumping to top level"
gc()

parallel::stopCluster (cl2)
