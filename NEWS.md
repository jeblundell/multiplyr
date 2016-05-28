# multiplyr 0.1.1
- Group boundaries are now cached and distributed by shared memory matrix
- Fixed group_by bug when using nycflights13 data
- Removed all calls to return from cluster_eval
- Implemented: add_rownames, between, cumall, cumany, cummean, first, lag, lead, n, n_distinct, n_groups, nth
- Extended bigmemory::morder functionality to allow mixes of ascending/descending (bm_morder and bm_mpermute)
- Made preparations for multiple data frames on same cluster

# multiplyr 0.1.0
- First version of package. See basics vignette for a good overview
