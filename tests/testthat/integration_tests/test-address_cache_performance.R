# Runs a test of cache performance using the fixture created by address_prepare_fixtures() as a small address_stream
# Requires an existing sqlite file in the primary stream cache already filled with address_parse data.

withr::local_envvar(R_CONFIG_FILE="")
withr::local_package("progressr")
future::plan("multisession")
handlers("cli")

address_stream <- readRDS(rprojroot::find_testthat_root_file("address_stream.Rds"))

single <- system.time(address_cache(address_stream, "address_parse", I))

future::plan("multisession")
multiple_100 <- system.time(address_cache_chunked(address_stream, "address_parse", I, n = 100))
multiple_1000 <- system.time(address_cache_chunked(address_stream, "address_parse", I, n = 1000))
multiple_10000 <- system.time(address_cache_chunked(address_stream, "address_parse", I, n = 10000))
single_100 <- system.time(address_cache_chunked(address_stream, "address_parse", I, n = 100, parallel = F))
single_1000 <- system.time(address_cache_chunked(address_stream, "address_parse", I, n = 1000, parallel = F))
single_10000 <- system.time(address_cache_chunked(address_stream, "address_parse", I, n = 10000, parallel = F))

runtimes <- rbind(multiple_100,multiple_1000,multiple_10000,
                  single_100,single_1000,single_10000) %>% as.data.frame %>%
  tibble::rownames_to_column() %>% setDT %>%
  .[,c("type","chunksize"):=data.table::tstrsplit(rowname,"_")] %>%
  .[,`:=`(chunksize = as.integer(chunksize),
          chunknum = nrow(address_stream)/as.integer(chunksize))]

test_that("parallel processing is faster than single threading", {
  expect_gt(single_100["elapsed"],multiple_100["elapsed"])
  expect_gt(single_1000["elapsed"],multiple_1000["elapsed"])
  expect_gt(single_10000["elapsed"],multiple_1000["elapsed"])
})

test_that("all runs are less than 30 seconds", {
  expect_true(all(runtimes$elapsed < 30))
})

test_that("runs are not O(N) in either the number of chunks or the length of the chunk", {
  expect_lt(lm(elapsed~chunksize,runtimes,type=="multiple")$coeff["chunksize"],.01)
  expect_lt(lm(elapsed~chunksize,runtimes,type=="single")$coeff["chunksize"],.01)
  # the number or chunks is slightly correlated...
  expect_lt(lm(elapsed~chunknum,runtimes,type=="multiple")$coeff["chunknum"],.2)
  expect_lt(lm(elapsed~chunknum,runtimes,type=="single")$coeff["chunknum"],.2)
})
