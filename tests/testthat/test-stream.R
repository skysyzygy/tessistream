withr::local_package("checkmate")
withr::local_package("mockery")

# stream ------------------------------------------------------------------

test_that("stream combines multiple streams into one", {
  n <- 100000
  stream_a <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                         timestamp = sample(seq(as.POSIXct("2023-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                            n,replace=T),
                         feature_a = runif(n)) 
  stream_b <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                         timestamp = sample(seq(as.POSIXct("2023-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                            n,replace=T),
                         feature_b = runif(n),
                         pk = sample(seq(n),n)) 
  
  read_cache <- mock(stream_a, stream_b)
  stream_chunk_write <- mock()
  stub(stream, "read_cache", read_cache)
  stub(stream, "stream_chunk_write", stream_chunk_write)
  stub(stream, "sync_cache", NULL)
  stub(stream, "cache_exists_any", FALSE)
  
  stream(streams = c("stream_a","stream_b"))
  
  expect_length(mock_args(stream_chunk_write),1)
  expect_equal(nrow(mock_args(stream_chunk_write)[[1]][[1]]),n*2)
  
})

test_that("stream works with mixed POSIXct/Date timestamps", {
  n <- 100000
  stream_a <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.POSIXct("2023-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_a = runif(n)) 
  stream_b <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.Date("2023-01-01"),as.Date("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_b = runif(n),
                                 pk = sample(seq(n),n)) 
  
  read_cache <- mock(stream_a, stream_b)
  stream_chunk_write <- mock()
  stub(stream, "read_cache", read_cache)
  stub(stream, "stream_chunk_write", stream_chunk_write)
  stub(stream, "sync_cache", NULL)
  stub(stream, "cache_exists_any", FALSE)
  
  stream(streams = c("stream_a","stream_b"))
  
  expect_length(mock_args(stream_chunk_write),1)
  expect_equal(nrow(mock_args(stream_chunk_write)[[1]][[1]]),n*2)
  
})

test_that("stream writes out partitioned dataset", {
  n <- 100000
  stream_a <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_a = runif(n)) 
  stream_b <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_b = runif(n),
                                 pk = sample(seq(n),n)) 
  
  read_cache <- mock(stream_a, stream_b)
  stream_chunk_write <- mock()
  stub(stream, "read_cache", read_cache)
  stub(stream, "stream_chunk_write", stream_chunk_write)
  stub(stream, "sync_cache", NULL)
  stub(stream, "cache_exists_any", FALSE)
  
  stream(streams = c("stream_a","stream_b"))
  
  expect_length(mock_args(stream_chunk_write),2)
  expect_equal(nrow(mock_args(stream_chunk_write)[[1]][[1]])+
                 nrow(mock_args(stream_chunk_write)[[2]][[1]]),n*2)
})


test_that("stream updates the existing dataset", {
  n <- 100000
  stream_a <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_a = runif(n)) 
  stream_b <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_b = runif(n),
                                 pk = sample(seq(n),n)) 
  stream_cache <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                     timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2022-08-31"),by="day"),
                                                        n,replace=T))
  
  read_cache <- mock(stream_a, stream_b, stream_cache)
  stream_chunk_write <- mock()
  stub(stream, "read_cache", read_cache)
  stub(stream, "stream_chunk_write", stream_chunk_write)
  stub(stream, "sync_cache", NULL)
  stub(stream, "cache_exists_any", TRUE)

  stream(streams = c("stream_a","stream_b"))
  
  stream <- rbindlist(list(collect(stream_a),collect(stream_b)),fill=T)
  stream_cache <- stream_cache %>% collect 
  expect_length(mock_args(stream_chunk_write),2)
  expect_equal(mock_args(stream_chunk_write)[[1]][[1]][,.N]+
               mock_args(stream_chunk_write)[[2]][[1]][,.N],
               stream[timestamp > max(stream_cache$timestamp),.N])
})

test_that("stream rebuilds the whole dataset if `rebuild=TRUE`", {
  n <- 100000
  stream_a <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_a = runif(n)) 
  stream_b <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                 timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                                    n,replace=T),
                                 feature_b = runif(n),
                                 pk = sample(seq(n),n)) 
  stream_cache <- arrow::arrow_table(group_customer_no = sample(seq(n/100),n,replace=T),
                                     timestamp = sample(seq(as.POSIXct("2022-01-01"),as.POSIXct("2022-08-31"),by="day"),
                                                        n,replace=T))
  
  read_cache <- mock(stream_a, stream_b, stream_cache)
  stream_chunk_write <- mock()
  stub(stream, "read_cache", read_cache)
  stub(stream, "stream_chunk_write", stream_chunk_write)
  stub(stream, "sync_cache", NULL)
  stub(stream, "cache_exists_any", TRUE)

  stream(streams = c("stream_a","stream_b"), rebuild = TRUE)
  
  stream <- rbindlist(list(collect(stream_a),collect(stream_b)),fill=T)
  stream_cache <- stream_cache %>% collect 
  expect_length(mock_args(stream_chunk_write),2)
  expect_equal(nrow(mock_args(stream_chunk_write)[[1]][[1]])+
                 nrow(mock_args(stream_chunk_write)[[2]][[1]]),
               2*n)
})

# stream_chunk_write ------------------------------------------------------

test_that("stream_chunk_write fills down all selected columns by group", {
  n <- 100000

  stream <- data.table(group_customer_no = sample(seq(n/100),n,replace=T),
                       timestamp = sample(seq(as.Date("2023-01-01"),as.Date("2023-12-31"),by="day"),
                                          n,replace=T),
                       feature_b = sample(c(NA,"b"),n,replace = T),
                       pk = sample(c(NA,"c"),n,replace=T),
                       year = 2023) 
  
  
  write_cache <- mock()
  stub(stream_chunk_write, "write_cache", write_cache)
  stub(stream_chunk_write, "cache_exists_any", F)

  stream_chunk_write(copy(stream), fill_cols = c("feature_b"))
  stream_actual <- mock_args(write_cache)[[1]][[1]]
  
  setkey(stream,group_customer_no,timestamp)
  first_b <- stream[!is.na(feature_b),which=T] %>% min
  
  expect_lt(stream_actual[is.na(feature_b),.N],stream[is.na(feature_b),.N])
  expect_equal(stream_actual[is.na(pk),.N],stream[is.na(pk),.N])
  
})


test_that("stream_chunk_write loads historical data", {
  n <- 100000
  
  stream <- data.table(group_customer_no = sample(seq(n/100),n,replace=T),
                       timestamp = sample(seq(as.POSIXct("2023-01-01"),as.POSIXct("2023-12-31"),by="day"),
                                          n,replace=T),
                       feature_b = sample(c(NA,"b"),n,replace = T),
                       pk = sample(c(NA,"c"),n,replace=T),
                       year = 2023) 
  
  stream[1, `:=`(group_customer_no = 1, timestamp = min(stream$timestamp)-1, pk = NA)]
  stream[group_customer_no == 1, feature_b := NA]
  
  write_cache <- mock()
  stub(stream_chunk_write, "write_cache", write_cache)
  stub(stream_chunk_write, "cache_exists_any", T)
  stub(stream_chunk_write, "stream_window_features", \(stream, ...) stream)
  stub(stream_chunk_write, "read_cache", arrow::arrow_table(timestamp = as_datetime("2022-01-01"),
                                                            group_customer_no = 1,
                                                            feature_b = "z"))
  stub(stream_chunk_write, "stream_customer_history", data.table(timestamp = as_datetime("2022-01-01"),
                                                                         group_customer_no = 1,
                                                                         pk = "x"))
  
  stream_chunk_write(copy(stream))
  
  stream_actual <- mock_args(write_cache)[[1]][[1]]
  setkey(stream_actual, group_customer_no, timestamp)
  
  expect_equal(stream_actual[group_customer_no == 1, unique(feature_b)],"z")
  expect_set_equal(stream_actual[group_customer_no != 1, unique(feature_b)],c(NA,"b"))
  expect_set_equal(stream_actual[group_customer_no == 1, unique(pk)],c("x","c"))
  expect_set_equal(stream_actual[group_customer_no != 1, unique(pk)],c(NA,"c"))
})

test_that("stream_chunk_write done once is the same as doing it multiple times", {
  
})

# stream_window_features --------------------------------------------------

test_that("stream_window_features constructs windowed features", {
  
  stream <- data.table(group_customer_no = rep(seq(100), each = 100),
                       timestamp = rep(as_datetime("2021-01-01") + ddays(seq(100)), 100),
                       feature = rep(seq(100), 100))

  stream_window <- stream_window_features(stream, windows = list(lubridate::days(1)))
  
  expect_contains(colnames(stream_window), "feature.-1")
  expect_equal(stream_window[,`feature.-1`], rep(c(rep(NA,1),rep(1,100-1)),100))

  stream_window <- stream_window_features(stream, windows = list(lubridate::days(30)))
  
  expect_contains(colnames(stream_window), "feature.-30")
  expect_equal(stream_window[,`feature.-30`], rep(c(rep(NA,30),rep(30,100-30)),100))
  
})

test_that("stream_window_features subtracts windowed features from each other", {
  
  stream <- data.table(group_customer_no = rep(seq(100), each = 100),
                       timestamp = rep(as_datetime("2021-01-01") + ddays(seq(100)), 100),
                       feature = rep(seq(100), 100))
  
  stream_window <- stream_window_features(stream, windows = list(lubridate::days(30), 
                                                                 lubridate::days(1),
                                                                 lubridate::days(90)))
  
  expect_contains(colnames(stream_window), c("feature.-1", "feature.-30", "feature.-90"))
  expect_equal(stream_window[,`feature.-1`], rep(c(rep(NA,1),rep(1,100-1)),100))
  expect_equal(stream_window[,`feature.-30`], rep(c(rep(NA,30),rep(29,100-30)),100))
  expect_equal(stream_window[,`feature.-90`], rep(c(rep(NA,90),rep(60,100-90)),100))
})

