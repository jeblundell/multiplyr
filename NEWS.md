## multiplyr 0.1.1
- Fixed "no function to return from" bug
- Group boundaries are now cached and distributed by shared memory matrix
- Fixed group_by bug, e.g. when using nycflights13 data
- arrange now supports desc(...)
- Implemented add_rownames, between, cumall, cumany, cummean, first, lag, lead, n, n_distinct, n_groups, nth
- Extended bigmemory::morder functionality to allow mixes of ascending/descending (bm_morder and bm_mpermute)
- Made preparations for multiple data frames on same cluster
- Added URL/BugReports to package description
- Added multiplyr.cores option (uses environment variable R_MULTIPLYR_CORES)
- Removed dependency on lazyeval

## multiplyr 0.1.0
- First version of package. See basics vignette for a good overview
