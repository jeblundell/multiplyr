#' dplyr-like package with shared memory matrices
#'
#' @description
#' dplyr-like package with shared memory matrices
#'
#' @section Standard dplyr-like functions:
#' \tabular{ll}{
#'     \code{\link{arrange}}     \tab Sort data frame \cr
#'     \code{\link{distinct}}    \tab Select unique rows or unique combinations of variables \cr
#'     \code{\link{filter}}      \tab Filter data \cr
#'     \code{\link{group_by}}    \tab Group data \cr
#'     \code{\link{group_sizes}} \tab Return size of groups \cr
#'     \code{\link{groupwise}}   \tab Use grouped data\cr
#'     \code{\link{mutate}}      \tab Change values of existing variables (and create new ones) \cr
#'     \code{\link{rename}}      \tab Rename variables \cr
#'     \code{\link{rowwise}}     \tab Use data as individual rows \cr
#'     \code{\link{select}}      \tab Retain only specified variables \cr
#'     \code{\link{sumarise}}    \tab Summarise data \cr
#'     \code{\link{transmute}}   \tab Change variables and drop all others \cr
#' }
#'
#' @section Parallel functions:
#' \tabular{ll}{
#'     \code{\link{partition}}       \tab Partition data evenly amongst cluster nodes \cr
#'     \code{\link{partition_group}} \tab Partition data so that each group is wholly on a node \cr
#'     \code{\link{within_group}}    \tab Execute code within a group \cr
#'     \code{\link{within_node}}     \tab Execute code within a group \cr
#' }
#'
#' @section Additional data frame functions:
#' \tabular{ll}{
#'     \code{\link{define}}      \tab Define new variables \cr
#'     \code{\link{fastdf}}      \tab Create new parallel data frame \cr
#'     \code{\link{fast_filter}} \tab Faster form of usual filter function, but has restrictions \cr
#'     \code{\link{undefine}}    \tab Delete variables \cr
#' }
#'
#' @section Utility functions:
#' \tabular{ll}{
#'     \code{\link{bind_variables}} \tab Bind parallel data frame variables to an environment \cr
#'     \code{\link{distribute}}     \tab Calculations for how to distribute x items over N nodes \cr
#'     \code{\link{group_restrict}} \tab Return a parallel data frame mapped to a particular group \cr
#' }
#'
#' @docType package
#' @name multiplyr
NULL
