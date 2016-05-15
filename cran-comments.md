## Test environments
* Local Linux (Debian) build, R 3.2.2

## R CMD check results
There were no ERRORs or WARNINGs. 

There will be a NOTE due to this being a first submission of the package.

There is a note for the non-standard directory 'man-roxygen'. I make use of
roxygen2's templates for documentation, so this is unavoidable.

There are 20 NOTES of the form "no visible binding for global variable",
which is spurious as those expressions are not executed in this instance
of R; the expressions are passed along to the relevant node via
clusterEvalQ and executed there, where there is a binding.

## Best practice deviation
I understand that it is preferred that functions do not do modification in
place and instead return a new, modified value of some operation. This package
deviates from that practice since the whole point is that underlying data is
a shared memory object: I hope this is made clearer by its implementation as a
reference class.

## Downstream dependencies
No downstream dependencies
