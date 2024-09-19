

#' stream
#' 
#' Combine all streams named in `streams` into a single dataset, filling down columns matched by `cols_match` and
#' optionally rebuilding the whole dataset if `rebuild` is `TRUE`. Windowed features are appended using `windows` 
#' as offsets.
#'
#' @param streams [character] vector of streams to combine
#' @param fill_match [character(1)] regular expression to use when matching columns to fill down 
#' @param window_match [character(1)] regular expression to use when matching columns to window
#' @param rebuild [logical(1)] whether or not to rebuild the whole dataset (`TRUE`) or just append to the end of it (`FALSE`)
#' @importFrom data.table setDT
#' @importFrom dplyr collect filter transmute
#' @importFrom tessilake read_cache cache_exists_any write_cache sync_cache
#' @importFrom checkmate assert_character assert_logical assert_list
#' @importFrom lubridate year as_datetime
#' @param ... not used
#' @return stream dataset as an [arrow::Table]
#' @export
stream <- function(streams = c("email_stream","ticket_stream","contribution_stream",
                                "membership_stream","ticket_future_stream","address_stream"),
                   fill_match = "^(email|ticket|contribution|membership|ticket|address).+(amt|level|count|max|min)",
                   window_match = "^(email|ticket|contribution|membership|ticket|address).+(count|amt)$",
                   rebuild = FALSE, ...) {
  
  . <- timestamp
  
  assert_character(streams,min.len = 1)
  assert_character(fill_match,len=1)
  assert_character(window_match,len=1)
  assert_logical(rebuild)
  
  # load stream headers
  streams <- lapply(streams, \(stream) read_cache(stream, "stream"))
  
  # match columns by name
  fill_cols <- lapply(streams, colnames) %>% unlist %>% unique %>% 
    grep(pattern = fill_match, ignore.case = T, perl = T, value = T)
  window_cols <- fill_cols %>% grep(pattern = window_match, ignore.case = T, perl = T, value = T)
  
  
  stream_max_date <- as.POSIXct("1900-01-01")
  if(cache_exists_any("stream","stream") & !rebuild) 
    stream_max_date <- read_cache("stream","stream") %>% summarise(max(timestamp)) %>% collect() %>% .[[1]]
  
  years <- lapply(streams, \(stream) filter(stream,timestamp > stream_max_date) %>% 
                    transmute(year(timestamp)) %>% collect) %>% 
    unlist %>% unique %>% sort
  
  for(.year in years) {
    
    # load data from streams
    stream <- lapply(streams, \(stream) filter(stream, timestamp > as.POSIXct(stream_max_date) & 
                                                 year(timestamp) == .year) %>% collect %>% setDT %>% 
                       .[,timestamp := lubridate::as_datetime(timestamp)]) %>%
      rbindlist(idcol = "stream", fill = T) %>% .[,year := .year]
    
    # do the filling and windowing
    stream_chunk_write(stream, fill_cols = fill_cols, window_cols = window_cols, since = stream_max_date, ...)
    
  }
  
  sync_cache("stream", "stream", overwrite = TRUE)
  
}


#' @describeIn stream Fill down cols in `stream_cols` and add windowed features to `stream` for timestamps after `since`
#' @importFrom checkmate assert_data_table assert_names assert_posixct assert_character assert_list
#' @importFrom lubridate as_datetime
#' @param stream [data.table] data to process and write 
#' @param fill_cols [character] columns to fill down 
#' @param window_cols [character] columns to window
#' @param since [POSIXct] only the data with timestamps greater than `since` will be written
#' @param by [character(1)] column name to group by for filling down and windowing
stream_chunk_write <- function(stream, fill_cols = setdiff(colnames(stream),
                                                             c(by, "timestamp")),
                               window_cols = fill_cols,
                               since = as.POSIXct(min(stream$timestamp)-1), 
                               by = "group_customer_no",
                               ...) {
  
  timestamp <- group_customer_no <- NULL
  
  assert_data_table(stream)
  assert_names(colnames(stream), must.include = c(by,"timestamp", fill_cols))
  assert_posixct(since,len=1)
  assert_character(by,len=1)

  stream_prev <- stream_customer_history <- data.table()
  if(cache_exists_any("stream","stream")) {
    
    # load data from prior year for windowing
    stream_prev <- read_cache("stream", "stream") %>% 
      filter(as_datetime(timestamp) >= as_datetime(since - dyears()) & 
               as_datetime(timestamp) <= as_datetime(since)) %>% 
      collect %>% setDT
    
    # and the last row per customer for filling down
    stream_customer_history <- stream_customer_history(read_cache("stream","stream"), 
                                                       by = by, 
                                                       before = since,
                                                       pattern = fill_cols) 
  }
  
  stream <- rbind(stream_customer_history,
                  stream_prev,
                  stream, fill = T) %>% setkey(group_customer_no, timestamp)
  
  # fill down
  setnafill(stream, type = "locf", cols = fill_cols, by = by)
  
  # window
  stream <- stream_window_features(stream, window_cols = window_cols, by = by, ...)
  
  # save
  write_cache(stream[timestamp > since], "stream", "stream", partition = "year", sync = FALSE, 
              incremental = TRUE, date_column = "timestamp")
}

#' @describeIn stream construct windowed features for columns in `stream_cols`, using a list of [lubridate::period], 
#' `windows` as offsets, and grouped by `by`
#' @param windows [lubridate::period] vector that determines the offsets used when constructing the windowed features.
stream_window_features <- function(stream, window_cols = setdiff(colnames(stream), 
                                                                 c("group_customer_no","timestamp")),
                                   windows = NULL, by = "group_customer_no") {
  
  timestamp <- NULL
  
  assert_data_table(stream)
  assert_names(colnames(stream), must.include = c(by, "timestamp",window_cols))
  assert_names(window_cols, disjunct.from = c(by, "timestamp"))
  assert_list(windows, types = "Period", null.ok = TRUE)
  
  all_cols <- union(window_cols,c(by,"timestamp"))
  
  if (length(windows) > 0) {
    # sort windows
    windows <- windows[order(purrr::map_dbl(windows, as.numeric))]
  }

  for (window in windows) {
    # rolling join with adjusted stream
    stream_rolled = copy(stream[,all_cols,with=F])[,timestamp := timestamp + window]
    stream <- stream_rolled[stream, on = c(by, "timestamp"), roll = Inf]
    
    # rename columns... i. columns are the original ones...
    new_cols <- paste0(window_cols, ".-", as.numeric(window)/86400)
    setnames(stream, window_cols, new_cols)
    setnames(stream, paste0("i.",window_cols), window_cols)
    stream[,(new_cols) := purrr::map2(window_cols, new_cols, \(.x,.y) get(.x)-get(.y))]
    
  }
  
  # work backwards through windowed features and subtract each from the next (lower offset) to 
  # get properly decoupled features
  for (i in rev(seq_along(windows)[-1])) {
    window <- windows[[i]]
    prev_window <- windows[[i-1]]
    
    new_cols <- paste0(window_cols, ".-", as.numeric(window)/86400)
    prev_cols <- paste0(window_cols, ".-", as.numeric(prev_window)/86400)
    
    stream[,(new_cols) := purrr::map2(new_cols, prev_cols, \(.x,.y) get(.x)-get(.y))]
  }
  
  stream
}
