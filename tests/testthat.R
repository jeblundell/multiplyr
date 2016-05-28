#Workaround for https://github.com/hadley/testthat/issues/144
Sys.setenv ("R_TESTS" = "")

library(testthat)
library(multiplyr)

test_check("multiplyr")

#Attempt to stop "no function to return from, jumping to top level"
gc()
