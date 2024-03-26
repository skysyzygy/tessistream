withr::local_package("mockery")
withr::local_package("checkmate")

.acq_dt <- lubridate::as_datetime(lubridate::today())
.responses <- c("Opened Email","Click Through","Hard Bounce","Soft Bounce","UnSubscribe")

# email_data --------------------------------------------------------------

email_data_stubbed <- function() {
  promotions <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotions.parquet"), as_data_frame = FALSE)
  promotion_responses <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotion_responses.parquet"), as_data_frame = FALSE)
  read_tessi <- mock(promotions, promotion_responses)

  stub(email_data, "read_tessi", read_tessi)

  email_data()
}

test_that("email_data loads from promotions and promotion_responses and returns an arrow table", {
  promotions <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotions.parquet"), as_data_frame = FALSE)
  promotion_responses <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotion_responses.parquet"), as_data_frame = FALSE)
  expect_class(email_data_stubbed(), "ArrowTabular")
  expect_equal(nrow(email_data_stubbed()), nrow(promotions) + nrow(promotion_responses))
})

# email_data_append -------------------------------------------------------

email_data_append_stubbed <- function(email_data) {
  responses <- arrow::arrow_table(response = 1:5, event_subtype = .responses)
  sources <- arrow::arrow_table(source_no = 1:1000, source_desc = cli::hash_md5(1:1000), acq_dt = .acq_dt)
  extractions <- arrow::arrow_table(source_no = 1:1000, extraction_desc = cli::hash_md5(paste("extraction",1:1000)))
  emails <- readRDS(rprojroot::find_testthat_root_file("email_stream-emails.RDs"))

  read_sql <- mock(responses, extractions)
  read_tessi <- mock(sources)
  stream_from_audit <- mock(emails)

  stub(email_data_append, "read_tessi", read_tessi)
  stub(email_data_append, "read_sql", read_sql)
  stub(email_data_append, "stream_from_audit", stream_from_audit)

  email_data_append(email_data)
}

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

test_that("email_data_append recalculates send timestamps based on earliest promote_dt / acq_dt", {
  email_data_append <- email_data_append_stubbed(email_data_stubbed()) %>% collect() %>% setDT
  email_data <- email_data_stubbed() %>% collect() %>% setDT()

  # Doesn't update response times
  email_compare_responses <- merge(
        email_data[!is.na(response),.(customer_no, source_no, timestamp)],
        email_data_append[event_subtype != "Send",.(customer_no, source_no, timestamp)],
        by = c("customer_no", "source_no")
  )

  expect_equal(email_compare_responses[,.(customer_no, source_no, timestamp = timestamp.x)] %>%
                 setkey(customer_no, source_no, timestamp),
               email_compare_responses[,.(customer_no, source_no, timestamp = timestamp.y)] %>%
                 setkey(customer_no, source_no, timestamp))

  # Does update promote dates
  first_responses <- email_data[,.(first_response = min(timestamp, na.rm=T)), by = "source_no"]

  email_compare_sends <- merge(
    first_responses,
    email_data_append[event_subtype == "Send",.(customer_no, source_no, timestamp)],
    by = c("source_no")
  )

  # to first response date
  expect_equal(email_compare_sends[source_no>1,timestamp],
               email_compare_sends[source_no>1,first_response])
  # or acq_dt
  expect_equal(email_compare_sends[source_no==1,timestamp],
               rep(.acq_dt,email_compare_sends[source_no==1,.N])) # variable set above

})

test_that("email_data_append fills in customer emails based on the send date", {
  email_data_append <- email_data_append_stubbed(email_data_stubbed()) %>% collect() %>% setDT
  emails <- readRDS(rprojroot::find_testthat_root_file("email_stream-emails.RDs")) %>% setDT %>%
    setkey(customer_no, timestamp)
  email_data <- email_data_stubbed() %>% collect() %>% setDT()

  # test that all given eaddresses are still present (can't check timestamps because they've been changed!)
  expect_contains(email_data_append[,eaddress],
                  email_data[,tolower(trimws(eaddress))])

  # filter out known eaddresses...
  email_data_append <- email_data_append[is.na(eaddress) | !eaddress %in% email_data[,tolower(trimws(eaddress))]]

  # build a test dataset that contains the first noted email
# first_emails <- rbind(emails,email_data_append[!is.na(eaddress)],fill=T) %>%
#     setkey(customer_no,timestamp) %>%
#     .[,.(address = head(coalesce(eaddress,address),1),
#          timestamp = as.POSIXct("1900-01-01")), by = "customer_no"]
#   # ... as well as the stream_from_audit table of email changes
#   emails <- rbind(emails, first_emails, fill = T) %>% setkey(customer_no, timestamp)
  # ... and turn it into something that can be joined without rolling
  emails <- emails[,.(eaddress = address, from = timestamp,
                      to = data.table::shift(timestamp, type = "lead",
                                             fill = Inf)), by = c("customer_no")]


  # test that all new eaddresses come from a matching address in the `emails` test set
  email_test <- merge(email_data_append,
                      emails, all.x = TRUE, by = c("customer_no", "eaddress"), allow.cartesian = TRUE) %>%
    .[,.(eaddress,customer_no,timestamp,from,to)]

  expect_length(email_test[!is.na(eaddress),
                           any(timestamp >= from & timestamp <= to),by = c("eaddress","timestamp")] %>%
                 .[is.na(V1) | !V1, unique(eaddress)],0)

  # test that missing eaddresses are before the addresses in the `emails` test set
  expect_true(merge(email_test[is.na(eaddress),.(max_timestamp = max(timestamp, na.rm=T)),by = "customer_no"],
                    email_test[,.(min_from = min(from, na.rm=T)), by = "customer_no"]) %>%
                  .[,all(max_timestamp < min_from)])

})

test_that("email_data_append cleans the email address and extracts the domain", {
  email_data_append <- email_data_append_stubbed(email_data_stubbed()) %>% collect() %>% setDT

  expect_no_match(email_data_append$eaddress, "[A-Z\\s]")
  expect_match(email_data_append[!is.na(eaddress),domain], "^(mac|me|hotmail|gmail|yahoo|bam)\\.")

})

# email_stream ------------------------------------------------------------

email_stream_stubbed <- function() {
  stub(email_stream,"email_data",email_data_stubbed)
  stub(email_stream,"email_data_append",email_data_append_stubbed)
  stub(email_stream,"write_cache",\(...){})
  stub(email_stream,"p2_stream",data.table(customer_no=integer(0)))

  email_stream()
}

test_that("email_stream is sane", {
  email_stream <- email_stream_stubbed() %>% collect %>% setDT()

  # no rows have been added or removed
  expect_equal(email_stream[,.N],email_data_stubbed() %>% collect %>% nrow)
  # all rows have a customer
  expect_equal(email_stream[is.na(group_customer_no),.N],0)
  # all rows have time information
  expect_equal(email_stream[is.na(timestamp),.N],0)
  # all rows have an email address
  #expect_equal(email_stream[is.na(eaddress),.N],0)

  expect_names(colnames(email_stream),
               must.include =
               c("group_customer_no", "customer_no", "timestamp", "event_type",
               "event_subtype", "source_no", "appeal_no", "campaign_no", "source_desc",
               "extraction_desc", "response", "url_no", "eaddress", "domain"))

})

test_that("email_stream returns an arrow table",{
  expect_class(email_stream_stubbed(),"ArrowTabular")
})

test_that("email_stream labels multiple opens as a forward",{
  email_stream <- email_stream_stubbed() %>% collect %>% setDT

  # There are no customer_no/source_no combinations with more than one open
  expect_equal(email_stream[grepl("open",event_subtype,ignore.case=T),
                  .N,by=c("customer_no","source_no")][N>1,.N],0)

  # And there are forwards that have been added
  expect_gt(email_stream[grepl("forward",event_subtype,ignore.case=T),.N],0)

})

test_that("email_stream adds subtype counts/min/max",{
  email_stream <- email_stream_stubbed() %>% collect %>% setDT
  expect_names(colnames(email_stream),
               must.include = data.table::CJ("email",
                                             gsub("\\s","_",tolower(c("Send","Forward",.responses))),
                                             c("count","timestamp_min","timestamp_max"),
                                             sep="_") %>%
                 do.call(what=paste))

  expect_true(email_stream[event_subtype == "Send", all(timestamp >= email_send_timestamp_min)])
  expect_true(email_stream[event_subtype == "Send", all(timestamp == email_send_timestamp_max)])
  expect_true(all(email_stream[event_subtype == "Send", email_send_count == 1:.N, by = "customer_no"]))
})


