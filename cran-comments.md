## Test environments
* Local Linux (Debian) build, R 3.2.2

## R CMD check results
There were no ERRORs or WARNINGs. 

There will be a NOTE due to this being a first submission of the package.

There are 20 NOTES of the form "no visible binding for global variable",
which is spurious as those expressions are not executed in this instance
of R; the expressions are passed along to the relevant node via
clusterEvalQ and executed there, where there is a binding.

## Downstream dependencies
No downstream dependencies
