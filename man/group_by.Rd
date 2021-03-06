% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/nse.R, R/ops.R
\name{group_by}
\alias{group_by}
\alias{group_by_}
\title{Group data}
\usage{
group_by(.self, ..., auto_partition = NULL)

group_by_(.self, ..., .dots, .cols = NULL, auto_partition = NULL)
}
\arguments{
\item{.self}{Data frame}

\item{...}{Additional parameters}

\item{auto_partition}{Re-partition across cluster after operation}

\item{.dots}{Workaround for non-standard evaluation}

\item{.cols}{Columns to group by (used internally)}
}
\value{
Data frame
}
\description{
Groups data by specified columns: further operations then work within those groups
}
\details{
Many data analysis problems require working with particular combinations
of data. For example, finding the average sales for a given day of the
week could be achieved with \code{group_by(day)} and
\code{summarise(sales = mean(sales)}. This would result in a data frame
with 7 rows (1 for each group) with the average sales stored in the
sales column.

Multiple grouping variables may be specified, separated by columns. The
above example could be extended to group by month as well as weekday,
e.g. \code{group_by(month, day)}. The resulting data frame would then
have 12 blocks of 7 (84 rows) with an average for each week day in that
month provided the same way as above.
}
\examples{
\donttest{
dat <- Multiplyr (x=1:100, G=rep(c("A", "B", "C", "D"), each=25))
dat \%>\% group_by (G) \%>\% summarise (N=length(x))
dat \%>\% shutdown()
}
}
\seealso{
Other row manipulations: \code{\link{arrange}},
  \code{\link{distinct}}, \code{\link{filter}},
  \code{\link{slice}}
}
