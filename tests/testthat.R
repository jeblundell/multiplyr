#Workaround for https://github.com/hadley/testthat/issues/144
Sys.setenv ("R_TESTS" = "")

library(testthat)
library(multiplyr)

test_check("multiplyr")
