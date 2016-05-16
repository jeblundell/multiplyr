## Resubmission
Uwe Ligges noted that the examples did not complete on win-builder. Looking at
00check.log from win-builder, it stops at the examples too (I had mistakenly 
presumed that the absence of complaint was the same as no complaint, sorry!)

* Same 3 NOTEs as before, no new ERRORs or WARNINGs
* Examples now explicitly shutdown cluster
* win-builder check now includes "DONE" with all examples and tests passing

## Test environments
* Linux (Debian stretch/sid x32), R 3.2.2
* Linux (Debian stretch/sid x32), R-devel r70617
* win-builder (x86_64-w64-mingw32), R-devel r70617
* Mac OS X (Darwin 13.4 x64), R 3.3.0

Linux build options: --as-cran --run-dontrun --run-donttest

OS X build options: --as-cran --no-manual.

## R CMD check results
### ERRORs
There were no ERRORs on all builds as outlined above.

There ar ERRORs when the check is run with --run-donttest due to several of the 
examples not specifying how many cores to allocate. The default is to allocate 
parallel::detectCores()-1 cores and the errors are due to an attempt to spawn 7 
R processes.

### WARNINGs
There were no WARNINGs for all builds

### NOTEs
There will be a NOTE due to this being a first submission of the package.

There is a NOTE for the non-standard directory 'man-roxygen'. I make use of
roxygen2's templates for documentation, so this is unavoidable.

There is a NOTE that repeats "no visible binding for global variable",
which is spurious as those expressions are not executed in this instance
of R; the expressions are passed along to the relevant node via
clusterEvalQ and executed there, where there is a binding.

The OS X build had 2 additional NOTEs due to absence of pandoc and GhostScript
on the test machine. 

## Downstream dependencies
No downstream dependencies due to this being the first submission
