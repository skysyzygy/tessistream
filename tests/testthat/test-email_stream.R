withr::local_package("mockery")
withr::local_package("checkmate")

# email_data --------------------------------------------------------------

test_that("email_data loads from promotions and promotion_responses and returns an arrow table", {
  promotions <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotions.parquet"), as_data_frame = FALSE)
  promotion_responses <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotion_responses.parquet"), as_data_frame = FALSE)
  expect_class(email_data_stubbed(), "ArrowTabular")
  expect_equal(nrow(email_data_stubbed()), nrow(promotions) + nrow(promotion_responses))
})


# email_data_append -------------------------------------------------------

test_that("email_data_append appends descriptive info for responses, sources, and extractions", {
  withr::local_package("dplyr")

  email_data_append <- email_data_append_stubbed(email_data_stubbed())
  expect_class(email_data_append,"ArrowTabular")
  email_data_append <- collect(email_data_append)

  expect_equal(n_distinct(email_data_append$event_subtype),6) # number of responses plus 1
  expect_equal(n_distinct(email_data_append$source_desc), # one description per source
               n_distinct(email_data_append$source_no))
  expect_equal(n_distinct(email_data_append$extraction_desc), # one description per source
               n_distinct(email_data_append$source_no))

})

# email_fix_timestamp -----------------------------------------------------

test_that("email_fix_timestamp recalculates send timestamps based on earliest promote_dt / acq_dt", {
  email_data_append <- email_data_append_stubbed(email_data_stubbed())
  email_fix_timestamp <- email_fix_timestamp(email_data_append) %>% collect %>% setDT
  email_data_append <- email_data_append %>% collect %>% setDT

  # Doesn't update response times
  email_compare_responses <- merge(
        email_fix_timestamp[!is.na(response),.(customer_no, source_no, timestamp)],
        email_data_append[event_subtype != "Send",.(customer_no, source_no, timestamp)],
        by = c("customer_no", "source_no")
  )

  expect_equal(email_compare_responses[,.(customer_no,source_no,timestamp = timestamp.x)] %>% setkey,
               email_compare_responses[,.(customer_no,source_no,timestamp = timestamp.y)] %>% setkey)

  # Does update promote dates
  email_compare_sends <- merge(
    email_fix_timestamp[event_subtype == "Send",.(customer_no, timestamp,
                           first_response = min(timestamp, na.rm=T)),
                        by = "source_no"],
    email_data_append[event_subtype == "Send",.(customer_no, source_no)],
    by = c("customer_no", "source_no")
  )

  # to first response date
  expect_equal(email_compare_sends[source_no>1,timestamp],
               email_compare_sends[source_no>1,first_response])
  # or acq_dt
  expect_equal(email_compare_sends[source_no==1,timestamp],
               rep(.acq_dt,email_compare_sends[source_no==1,.N])) # variable set above

})

# email_fix_eaddress ------------------------------------------------------

test_that("email_fix_eaddress fills in customer emails based on the send date", {
  email_fix_timestamp <- email_data_stubbed() %>% email_data_append_stubbed() %>%
    email_fix_timestamp %>% collect %>% setDT
  email_fix_eaddress <- email_fix_eaddress_stubbed(email_fix_timestamp)
  emails <- readRDS(rprojroot::find_testthat_root_file("email_stream-emails.Rds")) %>% setDT %>%
    setkey(customer_no, timestamp)

  # test that all given eaddresses are still present
  email_test_given <- merge(
    email_fix_timestamp[!is.na(eaddress),.(customer_no,timestamp,eaddress)],
    email_fix_eaddress[,.(customer_no,timestamp,eaddress)],
    all.x = TRUE
  ) %>% distinct %>% setkey

  expect_equal(email_test_given,
               email_fix_timestamp[!is.na(eaddress),.(customer_no,timestamp,eaddress)] %>% distinct %>% setkey)

  emails <- emails[,.(eaddress = tolower(trimws(address)), from = timestamp,
                      to = data.table::shift(timestamp, type = "lead", fill = Inf)),
                   by = c("customer_no")]

  # test that all new eaddresses come from a matching address in the `emails` test set
  email_test_new <- merge(
    email_fix_eaddress,
    emails, all.x = TRUE, allow.cartesian = TRUE,
    by = c("customer_no", "eaddress")) %>%
    .[,.(test = any(timestamp >= from & timestamp <= to)), by = c("eaddress","customer_no","timestamp")]

  expect_true(all(email_test_new[!is.na(test),test]))

  # test that missing eaddresses are before the addresses in the `emails` test set'
  email_test_missing <- merge(
    email_fix_eaddress[is.na(eaddress),.(max_timestamp = max(timestamp, na.rm=T)),by = "customer_no"],
    emails[,.(min_from = min(from, na.rm=T)), by = "customer_no"],
    all.x = TRUE
  )
  expect_true(email_test_missing[!is.na(max_timestamp),all(max_timestamp < min_from)])
})

test_that("email_fix_eaddress cleans the email address and extracts the domain", {
  email_fix_timestamp <- email_data_stubbed() %>% email_data_append_stubbed() %>%
    email_fix_timestamp
  email_fix_eaddress <- email_fix_eaddress_stubbed(email_fix_timestamp) %>%
    collect %>% setDT
  expect_no_match(email_fix_eaddress$eaddress, "[A-Z\\s]")
  expect_gt(email_fix_eaddress[!is.na(eaddress),.N],0)
  expect_match(email_fix_eaddress[!is.na(eaddress),domain],
               "^(mac|me|hotmail|gmail|yahoo|bam)\\.")
  expect_equal(email_fix_eaddress[,domain],
               stringr::str_replace(email_fix_eaddress[,eaddress],
                                    ".+@",""))
})

# email_subtype_features ---------------------------------------------------
#
# test_that("email_subtype_features labels multiple opens as a forward",{
#   email_stream <- email_data_stubbed() %>% email_data_append_stubbed() %>%
#     email_fix_timestamp() %>% email_fix_eaddress_stubbed() %>%
#     transmute(group_customer_no,customer_no,timestamp,
#               event_type = "Email", event_subtype,
#               source_no, appeal_no, campaign_no, source_desc, extraction_desc,
#               response, url_no, eaddress, domain) %>% compute
#
#   email_subtype_features <- email_subtype_features(email_stream) %>% collect %>% setDT
#
#   # There are no customer_no/source_no combinations with more than one open
#   expect_equal(email_subtype_features[grepl("open",event_subtype,ignore.case=T),
#                             .N,by=c("customer_no","source_no")][N>1,.N],0)
#
#   # And there are forwards that have been added
#   expect_gt(email_subtype_features[grepl("forward",event_subtype,ignore.case=T),.N],0)
#
# })

test_that("email_subtype_features adds subtype counts/min/max",{
  email_fix_timestamp <- email_data_stubbed() %>% email_data_append_stubbed() %>%
    email_fix_timestamp %>% compute

  email_subtype_features <- email_subtype_features(email_fix_timestamp) %>% collect %>% setDT

  expect_names(colnames(email_subtype_features),
               must.include = data.table::CJ("email",
                                             gsub("\\s","_",tolower(c("Send",.responses))),
                                             c("count","timestamp_min","timestamp_max"),
                                             sep="_") %>%
                 do.call(what=paste))

  subtypes <- email_subtype_features[,unique(event_subtype)]
  setkey(email_subtype_features, group_customer_no, timestamp)

  for(subtype in subtypes) {
    prefix <- paste0("email_",gsub("\\s+","_",tolower(subtype)))

    email_subtype_features[event_subtype == subtype,subtype_timestamp_min:=min(timestamp,na.rm=T), by = "group_customer_no"]

    # all features are filled after the first one
    expect_equal(email_subtype_features[timestamp >= subtype_timestamp_min &
                                          (is.na(get(paste0(prefix,"_timestamp_min"))) |
                                          is.na(get(paste0(prefix,"_timestamp_max"))) |
                                          is.na(get(paste0(prefix,"_count"))))],
                 email_subtype_features[integer(0)])
    # timestamp min must be less than current timestamp
    expect_equal(email_subtype_features[timestamp < get(paste0(prefix,"_timestamp_min"))],
                 email_subtype_features[integer(0)])
    # timestamp features are filled in correctly for matching subtypes
    expect_equal(email_subtype_features[event_subtype == subtype, timestamp],
                 email_subtype_features[event_subtype == subtype,
                                        get(paste0(prefix,"_timestamp_max"))],
    # 1us tolerance -- arrow timestamp is microsecond precision
                tolerance = 1e-6)
    # and non-matching subtypes
    expect_equal(email_subtype_features[event_subtype != subtype & timestamp < get(paste0(prefix,"_timestamp_max"))],
                 email_subtype_features[integer(0)])
    # and counts are just rank sequences
    expect_equal(email_subtype_features[event_subtype == subtype,
                                        .(group_customer_no, get(paste0(prefix,"_count")))] %>% setkey,
                 email_subtype_features[event_subtype == subtype,
                                        .(V2 = rank(factor(paste0(source_no,"|",timestamp),
                                                           levels = unique(paste0(source_no,"|",timestamp))),
                                                    ties.method = "max")),
                                        by="group_customer_no"] %>% setkey)

    email_subtype_features[,subtype_timestamp_min:=NULL]
  }

})

test_that("email_subtype_features run on a chunk returns the same as on a full dataset",{
  tessilake::local_cache_dirs()
  primary_keys = c("group_customer_no", "timestamp", "source_no", "event_subtype")

  email_fix_timestamp <- email_data_stubbed() %>% email_data_append_stubbed %>% email_fix_timestamp %>% compute

  email_subtype_features_full <- email_subtype_features(email_fix_timestamp) %>% collect %>% setDT
  email_fix_timestamp <- email_fix_timestamp %>% collect %>% setDT

  email_stream <- rbind(email_subtype_features_full[timestamp < mean(timestamp)],
                        email_fix_timestamp[timestamp >= mean(timestamp)],
                        fill = TRUE)

  write_cache(email_stream, "email_stream", "stream", overwrite = TRUE, primary_keys = primary_keys)

  email_stream_chunk <- read_cache("email_stream","stream") %>%
    filter(timestamp >= !!email_fix_timestamp[,mean(timestamp)])

  email_subtype_features_partial <- email_subtype_features(email_stream_chunk) %>% collect %>% setDT

  setorderv(email_subtype_features_partial, primary_keys)
  email_subtype_features_test <- email_subtype_features_full[timestamp >= mean(timestamp)]
  setorderv(email_subtype_features_test, primary_keys)

  expect_mapequal(email_subtype_features_partial,
                  email_subtype_features_test)

})

# email_stream_chunk ------------------------------------------------------------
email_stream_chunk <- NULL

test_that("email_stream_chunk returns arrow table", {
  tessilake::local_cache_dirs()
  primary_keys = c("source_no", "customer_no", "timestamp", "response")
  email_stream_chunk <<- email_stream_chunk_stubbed()
  # returns an arrow table
  expect_class(email_stream_chunk,"ArrowTabular")
})

test_that("email_stream_chunk returns all rows with basic info",{
  email_data <- email_data_stubbed() %>% collect %>% setDT
  email_stream_chunk <- email_stream_chunk %>% collect %>% setDT

  # no rows have been added or removed (except the p2 one)
  expect_equal(email_stream_chunk[,.N],email_data %>% nrow() + 1)
  # all rows have a customer
  expect_equal(email_stream_chunk[is.na(customer_no),.N],0)
  # all rows have time information
  expect_equal(email_stream_chunk[is.na(timestamp),.N],0)
  # all rows have campaign/source/appeal info (except the p2 one)
  expect_equal(email_stream_chunk[is.na(source_no),.N],0)
  expect_equal(email_stream_chunk[is.na(campaign_no),.N],1)
  expect_equal(email_stream_chunk[is.na(appeal_no),.N],1)

  expect_names(colnames(email_stream_chunk),
               must.include =
                 c("group_customer_no", "customer_no", "timestamp", "event_type",
                   "event_subtype", "source_no", "appeal_no", "campaign_no", "source_desc",
                   "extraction_desc", "response", "url_no", "email", "domain"))

})

test_that("email_stream_chunk returns all of the expected features", {
  email_stream_chunk <- email_stream_chunk %>% collect %>% setDT

  feature_cols <- data.table::CJ("email",
                                 gsub("\\s","_",tolower(c("Send",.responses))),
                                 c("count","timestamp_min","timestamp_max"),
                                 sep="_") %>% do.call(what=paste)


  expect_names(colnames(email_stream_chunk),
               must.include = feature_cols)

  for (col in feature_cols) {
    expect_gt(email_stream_chunk[!is.na(get(col)),.N],email_stream_chunk[,.N]/20)
  }

})

test_that("email_stream_chunk returns the same result when run with one or many chunks",{
  tessilake::local_cache_dirs()

  email_data <- email_stream_chunk %>% select(timestamp) %>% collect %>% setDT
  setkey(email_data,timestamp)
  email_data[,group := cut(seq(.N),breaks=5,labels=F)]

  purrr::imap(split(email_data,by="group"),
    \(group, i) {
        expr <- quote(email_stream_chunk_stubbed(from_date = group[,min(timestamp,na.rm=T)],
                                 to_date = group[,max(timestamp,na.rm=T)+1000]))
      if (i == 1) {
        eval(expr)
      } else {
        expect_warning(!!expr)
      }
    }
  )

  rows <- sample(nrow(email_data), 1000)

  email_stream <- read_cache("email_stream","stream") %>% collect %>% setDT %>%
    setkey(group_customer_no,timestamp,source_no,event_subtype, appeal_no, campaign_no)
  email_stream_expected <- email_stream_chunk %>% collect %>% setDT %>%
    setkey(group_customer_no,timestamp,source_no,event_subtype, appeal_no, campaign_no)

  expect_equal(email_stream[rows,], email_stream_expected[rows,],
               ignore_attr = c("partition_key", "sorted"),
               list_as_map = TRUE)

})


# email_stream ------------------------------------------------------------

test_that("email_stream executes email_stream_chunk by year up while honoring from_date and to_date", {
  withr::local_package("lubridate")
  email_stream_chunk <- mock()
  stub(email_stream,"email_stream_chunk",email_stream_chunk)
  stub(email_stream,"sync_cache",TRUE)

  email_stream(from_date = make_datetime(2020,7,1), to_date = make_datetime(2024,5,30))

  expect_length(mock_args(email_stream_chunk), 5)
  expect_equal(mock_args(email_stream_chunk)[[1]][["from_date"]], make_datetime(2020,7,1))
  for (i in seq(2,5)) {
    expect_equal(mock_args(email_stream_chunk)[[i]][["from_date"]], make_datetime(2019+i))
  }
  for (i in seq(1,4)) {
    expect_equal(mock_args(email_stream_chunk)[[i]][["to_date"]], make_datetime(2020+i)-.001)
  }
  expect_equal(mock_args(email_stream_chunk)[[5]][["to_date"]], make_datetime(2024,5,30))

})


