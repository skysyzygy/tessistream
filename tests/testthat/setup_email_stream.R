# test fixtures for test-email_stream.R and integration_tests/test-email_prospect2_stream.R

.acq_dt <- lubridate::as_datetime(lubridate::today())
.responses <- c("Open","Click","Hard Bounce","Soft Bounce","Unsubscribe")

email_data_stubbed <- function(...) {
  promotions <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotions.parquet"), as_data_frame = FALSE)
  promotion_responses <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotion_responses.parquet"), as_data_frame = FALSE)
  tessilake:::cache_set_attributes(promotions, list(primary_keys = c("source_no","customer_no")))
  tessilake:::cache_set_attributes(promotion_responses, list(primary_keys = c("ID")))

  read_tessi <- mock(promotion_responses, promotions, cycle=T)

  stub(email_data, "read_tessi", read_tessi)

  email_data(...)
}

email_data_append_stubbed <- function(email_data) {
  responses <- arrow::arrow_table(response = 1:5, event_subtype = .responses)
  sources <- arrow::arrow_table(source_no = 1:1000, source_desc = cli::hash_md5(1:1000), acq_dt = .acq_dt)
  extractions <- arrow::arrow_table(source_no = 1:1000, extraction_desc = cli::hash_md5(paste("extraction",1:1000)))

  read_sql <- mock(responses, extractions)
  read_tessi <- mock(sources)

  stub(email_data_append, "read_tessi", read_tessi)
  stub(email_data_append, "read_sql", read_sql)

  email_data_append(email_data)
}

email_fix_eaddress_stubbed <- function(email_stream) {
  emails <- readRDS(rprojroot::find_testthat_root_file("email_stream-emails.Rds")) %>% setDT
  stream_from_audit <- mock(emails)
  stub(email_fix_eaddress, "stream_from_audit", stream_from_audit)

  email_fix_eaddress(email_stream)
}


email_stream_base_stubbed <- function(...) {
  stub(email_stream_base,"email_data",email_data_stubbed)
  stub(email_stream_base,"email_data_append",email_data_append_stubbed)
  stub(email_stream_base,"email_fix_eaddress",email_fix_eaddress_stubbed)

  email_stream_base(...)
}

email_stream_chunk_stubbed <- function(...) {
  stub(email_stream_chunk,"email_stream_base",email_stream_base_stubbed)
  write_cache(data.frame(group_customer_no=0L,
                         customer_no=0L,
                         campaignid=0L,
                         timestamp=as.POSIXct(Sys.Date()),
                         event_subtype="Send"),
              "p2_stream","stream",overwrite = TRUE)

  email_stream_chunk(...)
}
