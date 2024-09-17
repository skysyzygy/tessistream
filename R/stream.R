

#' stream
#' 
#' Combine all streams named in `streams` into a single dataset, filling down columns matched by `cols_match` and
#' optionally rebuilding the whole dataset if `rebuild` is `TRUE`. Windowed features are appended
#'
#' @param streams [character] vector of streams to combine
#' @param cols_match [character(1)] regular expression to use when matching columns to fill down 
#' @param rebuild [logical(1)] whether or not to rebuild the whole dataset (`TRUE`) or just append to the end of it (`FALSE`)
#' @param windows [lubridate::period] vector that determines the offsets used when constructing the windowed features.
#' @importFrom data.table setDT
#' @importFrom dplyr collect filter transmute
#' @importFrom tessilake read_cache cache_exists_any write_cache sync_cache
#' @importFrom lubridate year as_datetime
#' @return stream dataset as an [arrow::Table]
#' @export
stream <- function(streams = c("email_stream","ticket_stream","contribution_stream",
                                "membership_stream","ticketFuture_stream","address_stream"),
                   cols_match = "^(email|ticket|contribution|membership|ticket|address).+(amt|level|count|max|min)",
                   windows = lapply(c(1,7,30,90,365),lubridate::days),
                   rebuild = FALSE) {
  
  assert_character(streams,min.len = 1)
  assert_logical(rebuild)
  assert_list(windows, types = "Period")
  
  # load stream headers
  streams <- lapply(streams, \(stream) read_cache(stream, "stream"))
  
  # match columns by name
  stream_cols <- lapply(streams, colnames) %>% unlist %>% unique %>% 
    grep(pattern = cols_match, ignore.case = T, perl = T)

  stream_max_date <- as.POSIXct(-Inf)
  if(cache_exists_any("stream","stream") & !rebuild) 
    stream_max_date <- read_cache("stream","stream") %>% summarise(max(timestamp)) %>% collect() %>% .[[1]]
  
  years <- lapply(streams, \(stream) filter(stream,timestamp > stream_max_date) %>% 
                    transmute(year(timestamp)) %>% collect) %>% 
    unlist %>% unique
  
  for(.year in years) {
    
    # load data from streams
    stream <- lapply(streams, \(stream) filter(stream, timestamp > as.POSIXct(stream_max_date) & 
                                                 year(timestamp) == .year) %>% collect %>% setDT %>% 
                       .[,timestamp := as.POSIXct(timestamp)]) %>%
      rbindlist(idcol = "stream", fill = T) %>% .[,year := .year]
    
    stream_chunk_write(stream, cols = stream_cols, since = stream_max_date, windows = windows)
    
  }
  
  sync_cache("stream", "stream", overwrite = TRUE)
  
}


#' @describeIn stream Fill down cols in `stream_cols` and add windowed features to `stream` for timestamps after `since`
stream_chunk_write <- function(stream, stream_cols = setdiff(colnames(stream),
                                                             c(by, "timestamp")),
                               since = min(stream$timestamp), 
                               by = "group_customer_no",
                               windows = NULL) {
  
  assert_data_table(stream)
  assert_names(colnames(stream), must.include = c(by,"timestamp"))
  
  stream_prev <- stream_customer_history <- data.table()
  if(cache_exists_any("stream","stream")) {
    # load data from prior year for windowing
    stream_prev <- read_cache("stream", "stream") %>% 
      filter(as_datetime(timestamp) >= as_datetime(since - dyears())) %>% 
      collect %>% setDT
    stream_customer_history <- stream_customer_history(read_cache("stream","stream"), 
                                                       by = by, 
                                                       cols = stream_cols) 
  }
  
  stream <- rbind(stream_customer_history,
                  stream_prev,
                  stream, fill = T) %>% setkey(group_customer_no, timestamp)
  
  # fill down
  setnafill(stream, type = "locf", cols = intersect(colnames(stream), stream_cols), by = by)
  
  # window
  stream_window_features(stream, stream_cols = stream_cols, windows = windows, by = by)
  
  # save
  write_cache(stream[timestamp > since], "stream", "stream", partition = "year", sync = FALSE, 
              overwrite = TRUE, incremental = FALSE)
}

#' @describeIn stream construct windowed features for columns in `stream_cols`, using a list of [lubridate::period], 
#' `windows` as offsets, and grouped by `by`
stream_window_features <- function(stream, stream_cols = setdiff(colnames(stream), 
                                                                 c("group_customer_no","timestamp")),
                                   windows = NULL, by = "group_customer_no") {
  
  assert_data_table(stream)
  assert_names(colnames(stream), must.include = c(by, "timestamp",stream_cols))
  assert_names(stream_cols, disjunct.from = c(by, "timestamp"))
  assert_list(windows, types = "Period", null.ok = TRUE)
  
  all_cols <- union(stream_cols,c(by,"timestamp"))
  
  if (length(windows) > 0) {
    # sort windows
    windows <- windows[order(purrr::map_dbl(windows, as.numeric))]
  }
  last_window = NULL
  for (window in windows) {
    # rolling join with adjusted stream
    stream_rolled_forward = copy(stream[,..all_cols])[,timestamp := timestamp + window]
    stream <- stream_rolled_forward[stream, on = c(by, "timestamp"), roll = Inf]
    
    # rename columns
    window_cols <- paste0(stream_cols, "_-", as.numeric(window)/86400)
    setnames(stream, paste0("i.",stream_cols), window_cols)
    
    # subtract from the previous
    if (!is.null(last_window)) {
      stream[,(window_cols) := mget(window_cols) - mget(last_window_cols)]
    }
    
    last_window = window
    last_window_cols = window_cols
  }
  
}
