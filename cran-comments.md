## Resubmission
* Examples now explicitly shutdown cluster
* win-builder check now includes "DONE" with all examples and tests passing
* Added man-roxygen to .Rbuildignore
* utils::globalVariables for expressions passed to cluster

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

There are ERRORs when the check is run with --run-donttest due to several of the
examples not specifying how many cores to allocate. The default is to allocate 
parallel::detectCores()-1 cores and the errors are due to an attempt to spawn 7 
R processes.

### WARNINGs
There were no WARNINGs for all builds

### NOTEs
There will be a NOTE due to this being a first submission of the package.

The OS X build had 2 additional NOTEs due to absence of pandoc and GhostScript
on the test machine. 

## Downstream dependencies
No downstream dependencies due to this being the first submission
