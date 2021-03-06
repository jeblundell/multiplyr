% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ops.R
\name{within_node}
\alias{within_node}
\title{Execute code within a node}
\usage{
within_node(.self, expr)
}
\arguments{
\item{.self}{Data frame}

\item{expr}{Code to execute}
}
\description{
This is the mainstay of parallel computation for a data frame. This will
execute the specified expression within each node. Each node will have a
persistent environment, so that variables created in that environment can
be referred to by, for example, later calls to summarise. This environment
contains active bindings to the columns of that data frame.
}
\seealso{
Other data manipulations: \code{\link{mutate}},
  \code{\link{nsa}}, \code{\link{reduce}},
  \code{\link{summarise}}, \code{\link{transmute}},
  \code{\link{within_group}}
}
