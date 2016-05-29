## Changes since last version
- Changed maintainer email (address expires in a couple of months)
- Fixed "no function to return from" bug
- Changed underlying grouping methods
- Implemented various minor helper functions
- Added URL/BugReports to package description

## Test environments
- Local Linux (Debian stretch/sid), with R 3.2.2
- Travis Linux (Ubuntu 12.04.5), with R 3.2.5
- Travis Linux (Ubuntu 12.04.5), with R 3.3.0
- Travis Linux (Ubuntu 12.04.5), with R r70682
- win-builder, with R 3.3.0
- win-builder, with R r70682

## R CMD check results
### ERRORs
ERRORs occur when --run-dontrun and --run-donttest are used with --as-cran as 
the default behaviour is to use all available cores for parallel processing and 
this results in exceeding the limit. The tests otherwise pass.

### WARNINGs
No WARNINGs

### NOTEs
1 NOTE due to change of maintainer email address

## Downstream dependencies
No downstream dependencies as of 29/05/2016
