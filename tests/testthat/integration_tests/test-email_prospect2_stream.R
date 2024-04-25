withr::local_package("mockery")
withr::local_package("checkmate")
source_test_setup("..",env=rlang::current_env())

test_that("email_subtype_features works with combined email_stream/p2_stream dataset", {

  email_stream <- email_data_stubbed() %>% email_data_append_stubbed() %>%
    email_fix_timestamp() %>% email_fix_eaddress_stubbed() %>%
    transmute(group_customer_no,customer_no,timestamp,
              event_type = "Email", event_subtype,
              source_no, appeal_no, campaign_no, source_desc, extraction_desc,
              response, url_no, eaddress, domain) %>% collect

  withr::local_envvar(R_CONFIG_FILE="")
  p2_stream <- read_cache("p2_stream","stream") %>%
    select(group_customer_no, timestamp, campaignid, event_subtype) %>% collect
  p2_stream[, source_no := -campaignid]

  email_subtype_features <- rbindlist(list(email_stream, p2_stream), fill = T) %>%
    email_subtype_features() %>%
    collect %>% setDT

  # There are no customer_no/source_no combinations with more than one open
  expect_equal(email_subtype_features[grepl("open",event_subtype,ignore.case=T),
                                      .N,by=c("group_customer_no","source_no")][N>1,.N],0)

  # And there are forwards that have been added
  expect_gt(email_subtype_features[grepl("forward",event_subtype,ignore.case=T),.N],0)

  #email_subtype_features adds subtype counts/min/max"
  expect_names(colnames(email_subtype_features),
               must.include = data.table::CJ("email",
                                             gsub("\\s","_",tolower(c("Send","Forward",.responses))),
                                             c("count","timestamp_min","timestamp_max"),
                                             sep="_") %>%
                 do.call(what=paste))

  subtypes <- email_subtype_features[,unique(event_subtype)]

  for(subtype in subtypes) {
    prefix <- paste0("email_",gsub("\\s+","_",tolower(subtype)))

    email_subtype_features[event_subtype == subtype,subtype_timestamp_min:=min(timestamp,na.rm=T), by = "customer_no"]

    # all features are filled after the first one
    expect_equal(email_subtype_features[timestamp >= subtype_timestamp_min &
                                          (is.na(get(paste0(prefix,"_timestamp_min"))) |
                                             is.na(get(paste0(prefix,"_timestamp_max"))) |
                                             is.na(get(paste0(prefix,"_count"))))],
                 email_subtype_features[integer(0)])
    # timestamp min must be less than current timestamp
    expect_equal(email_subtype_features[timestamp < get(paste0(prefix,"_timestamp_min"))],
                 email_subtype_features[integer(0)])
    # features are filled in correctly for matching
    expect_equal(email_subtype_features[event_subtype == subtype & timestamp != get(paste0("email_",gsub("\\s+","_",tolower(subtype)),"_timestamp_max"))],
                 email_subtype_features[integer(0)])
    # and non-matching subtypes
    expect_equal(email_subtype_features[event_subtype != subtype & timestamp < get(paste0("email_",gsub("\\s+","_",tolower(subtype)),"_timestamp_max"))],
                 email_subtype_features[integer(0)])
    # and counts are just integer sequences
    expect_equal(email_subtype_features[event_subtype == subtype, .(group_customer_no, get(paste0("email_",gsub("\\s+","_",tolower(subtype)),"_count")))] %>% setkey,
                 email_subtype_features[event_subtype == subtype,.(V2 = 1:.N),by="group_customer_no"] %>% setkey)

    email_subtype_features[,subtype_timestamp_min:=NULL]
  }

})
