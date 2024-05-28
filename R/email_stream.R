#' @title email_stream
#' @description
#'
#' Combined dataset of email sends/clicks/opens from Tessitura / WordFly and data from [p2_stream].
#' Features included are:
#'
#' * group_customer_no, customer_no
#' * timestamp : date of email event
#' * event_type : "Email"
#' * event_subtype : "Open", "Click", "Unsubscribe', "Hard Bounce", "Forward", etc.
#' * campaign_no, appeal_no, source_no
#' * extraction_desc, source_desc
#' * eaddress : email address
#' * domain : domain of email (everything after the `@`)
#' * email_\[subtype\]_count
#' * email_\[subtype\]_timestamp_min
#' * email_\[subtype\]_timestamp_max

#' @name email_stream
#' @returns [arrow::Table] of email data
NULL


#' @describeIn email_stream load Tessitura email send/response data
#' @importFrom tessilake read_sql read_tessi
#' @importFrom dplyr filter compute collect summarise
#' @importFrom data.table tstrsplit
#' @inheritDotParams tessilake::read_sql freshness
#' @importFrom arrow concat_tables
#' @param from_date earliest date/time for which data will be returned
#' @param to_date latest date/time for which data will be returned
email_data <- function(..., from_date = as.POSIXct("1900-01-01"), to_date = now()) {

  media_type <- promote_dt <- group_customer_no <- customer_no <- eaddress <-
    appeal_no <- campaign_no <- source_no <- response_dt <- response <- url_no <- NULL

  ### Promotions

  promotions = read_tessi("promotions", ...) %>%
    filter(media_type == 3) %>%
    filter(promote_dt >= from_date & promote_dt < to_date) %>%
    select(group_customer_no,customer_no,eaddress,
           promote_dt,appeal_no,campaign_no,source_no) %>%
    compute

  ### Promotion responses

  promotion_responses = read_tessi("promotion_responses", ...) %>%
    filter(response_dt >= from_date & response_dt < to_date) %>%
    transmute(group_customer_no,customer_no,
              response, timestamp = response_dt,
              source_no, url_no) %>%
    left_join(summarise(promotions,
                        appeal_no = max(appeal_no),
                        campaign_no = max(campaign_no),
                        .by = source_no)) %>%
    compute

  ### Assemble email_stream

  email_stream <- concat_tables(promotions, promotion_responses, unify_schemas = TRUE)

}

#' @describeIn email_stream adds descriptive campaign/appeal/source information from Tessitura
#' @param email_stream email data from previous step
email_data_append <- function(email_stream, ...) {

  source_no <- source_desc <- acq_dt <- NULL

  ### Response descriptions

  responses = read_sql("select email_response response,
                        description event_subtype from TR_EMAIL_RESPONSE_CODE erc
                        join TR_RESPONSE r on
                        erc.promo_response=r.id", ...)

  ### Sources and extractions

  sources = read_tessi("sources", ...) %>% select(source_no, source_desc, acq_dt)
  extractions = read_sql("select distinct source_no,
                          description extraction_desc from BIExtract.tw_keycode_detail kd
                          join BIExtract.t_ka_header ka on kd.ka_no=ka.ka_no and
                          source_no is not null", ...)

  email_stream <- email_stream %>%
    left_join(responses, by = "response") %>%
    # set un-labeled events as sends
    mutate(event_subtype = coalesce(event_subtype,"Send")) %>%
    left_join(sources, by = "source_no") %>%
    left_join(extractions, by = "source_no") %>%
    compute
}

#' @importFrom dplyr mutate summarise left_join coalesce collect
#' @describeIn email_stream recalculates timestamps for sends because `promote_dt` is often long before
#' the actual email send date.
email_fix_timestamp <- function(email_stream) {

  timestamp <- source_no <- first_response_dt <- acq_dt <- promote_dt <- NULL

  ### Fix timestamp for sends/promotions

  # recalculate timestamps because promotion often happens long before the actual send
  # NOTE: Have to bring this into R because window aggregation not implemented in Arrow
  first_response <- email_stream %>% filter(!is.na(timestamp)) %>%
    summarise(first_response_dt = min(timestamp, na.rm = TRUE),.by = source_no) %>%
    collect

  email_stream <- email_stream %>% left_join(first_response, by = "source_no") %>%
    # 1. use first recorded response for source
    # 2. use acq_dt from the source code because this is sometimes set manually as a replacement for acq_dt
    # 3. use promote_dt
    mutate(timestamp = coalesce(timestamp,first_response_dt,acq_dt,promote_dt),
    # Arrow uses int64 for timestamps; R uses double precision floating points.
    # to avoid precision loss and failed joins, create a timestamp_id for joins
           timestamp_id = arrow:::cast(timestamp, arrow::int64()))
}

#' @importFrom dplyr select collect compute
#' @describeIn email_stream fills in email address based on time of send and the current email address for the customer,
#' using email data from [stream_from_audit]
email_fix_eaddress <- function(email_stream) {
  . <- address <- primary_ind <- customer_no <- group_customer_no <- timestamp <- eaddress <- i.address <- domain <- NULL

  ### Email addresses
  emails <- stream_from_audit("emails") %>%
    .[!is.na(address) & address>0 & primary_ind=="Y"]

  # Match email address based on customer_no (or group_customer_no) and send date using a rolling join
  # NOTE: Have to bring this into R because inequality joins and rolling joins not implemented in Arrow
  email_matches <- email_stream %>%
    transmute(customer_no, group_customer_no, timestamp, timestamp_id, eaddress) %>%
    collect %>% setDT %>% stream_debounce("customer_no", "timestamp")

  email_matches <- emails[,.(customer_no,timestamp,address)][email_matches,on=c("customer_no","timestamp"),roll=T]
  email_matches <- emails[,.(group_customer_no,timestamp,address)][email_matches,on=c("group_customer_no","timestamp"),roll=T]

  # clean email address and extract domain
  email_matches[,eaddress := stringr::str_trim(tolower(coalesce(eaddress, address, i.address)))]
  emails_cleaned <- data.table(eaddress = unique(email_matches$eaddress)) %>%
    .[,domain := stringr::str_replace(eaddress, ".+@", "")]
  setleftjoin(email_matches, emails_cleaned, by = "eaddress")

  # provide event_type and event_subtype and remove columns
  email_stream <- email_stream %>% select(-eaddress) %>% compute %>%
    left_join(email_matches[,.(customer_no,timestamp_id,eaddress,domain)],
              by = c("customer_no","timestamp_id"))

}


#' @importFrom dplyr select collect compute
#' @importFrom tidyr any_of
#' @describeIn email_stream sets multiple `event_subtype == "open"` as `"forward"` and builds windowed features
#' for each `event_subtype`
email_subtype_features <- function(email_stream) {
  group_customer_no <- timestamp <- source_no <- event_subtype <- . <- NULL

  # must be an arrow query in order to extract customer history from the full dataset
  assert_multi_class(email_stream, c("arrow_dplyr_query","ArrowTabular"))

  # working data.table
  email_subtypes <- email_stream %>%
    select(group_customer_no, timestamp, timestamp_id, source_no, event_subtype) %>%
    collect %>% setDT

  min_timestamp <- email_subtypes[,min(timestamp)]
  subtypes = unique(na.omit(email_subtypes$event_subtype))
  feature_cols = paste("email",gsub("\\s","_",tolower(subtypes)),
                       rep(c("timestamp_min", "timestamp_max", "count"), each = length(subtypes)),
                       sep="_")

  group_customer_nos <- distinct(email_subtypes[,.(group_customer_no)])

  # generate customer history from the overall dataset
  customer_history <- if ("arrow_dplyr_query" %in% class(email_stream)) {
     email_stream$.data %>%
      filter(group_customer_no %in% group_customer_nos$group_customer_no) %>%
      stream_customer_history("group_customer_no",
                              before = min_timestamp,
                              pattern = "count|min|max$")
  } else {
    group_customer_nos
  }

  # make customer_history have one row per group_customer_no
  customer_history <- customer_history[group_customer_nos,on="group_customer_no"]
  email_subtypes <- rbind(customer_history, email_subtypes, fill = T) %>%
    setkey(group_customer_no, timestamp)

  for( subtype in subtypes ) {
    # email_[subtype]_timestamp_min
    # email_[subtype]_timestamp_max
    # email_[subtype]_count

    cols = paste("email",gsub("\\s","_",tolower(subtype)),
                 c("timestamp_min", "timestamp_max", "count"),sep="_")
    i_cols = paste0("i.",cols)
    customer_history[,event_subtype := subtype]

    # extrema features
    email_subtypes[customer_history,
                   (i_cols) := mget(i_cols, ifnotfound = NA),
                   on=c("group_customer_no","event_subtype")] %>%
      .[event_subtype == subtype,
                   (cols) := list(coalesce(get(i_cols[1]), timestamp[1]),
                                  timestamp,
                                  coalesce(get(i_cols[3]),0) + seq_len(.N)),
                   by="group_customer_no"] %>%
      .[,(i_cols) := NULL]
  }

  # Fill down, respecting customer boundaries
  setnafill(email_subtypes,
            type = "locf",
            cols = feature_cols,
            by = "group_customer_no")

  # remove added customer history and debounce so that it can be left-joined
  email_subtypes <- email_subtypes[timestamp >= min_timestamp] %>%
    stream_debounce(c("group_customer_no", "timestamp", "source_no", "event_subtype"))

  email_stream %>%
    select(!any_of(c("timestamp",feature_cols))) %>%
    left_join(email_subtypes,
              by=c("group_customer_no", "timestamp_id", "source_no", "event_subtype"))

}


email_stream_chunk <- function(..., from_date = as.POSIXct("1900-01-01"), to_date = now()) {

  customer_no <- timestamp <- event_subtype <- source_no <-
    appeal_no <- campaign_no <- source_desc <- extraction_desc <-
    response <- url_no <- eaddress <- domain <- campaignid <- NULL

  email_stream <- email_data(..., from_date, to_date) %>% email_data_append(...) %>%
    email_fix_timestamp %>% email_fix_eaddress %>%
    transmute(group_customer_no = as.integer(group_customer_no), customer_no,
              timestamp = lubridate::force_tz(timestamp,"America/New_York"),
              timestamp_id, event_type = "Email", event_subtype,
              source_no, appeal_no, campaign_no, source_desc, extraction_desc,
              response, url_no, email = eaddress, domain) %>% compute

  if (cache_exists_any("p2_stream","stream")) {
    p2_stream <- read_cache("p2_stream","stream") %>%
      filter(timestamp >= from_date & timestamp < to_date) %>%
      # p2_stream doesn't have source_no, which is used in
      # email_subtype_features for sequential features.
      mutate(source_no = -campaignid,
     # Arrow uses int64 for timestamps; R uses double precision floating points.
     # to avoid precision loss and failed joins, create a timestamp_id for joins
             timestamp_id = arrow:::cast(timestamp, arrow::int64())) %>% compute
    # update the dataset with the p2 data
    email_stream <- arrow::concat_tables(email_stream,p2_stream,unify_schemas = T)
  }

  email_stream_write_partition(email_stream, primary_keys)

  email_stream <- read_cache("email_stream","stream") %>%
    filter(timestamp >= from_date & timestamp < to_date) %>%
    email_subtype_features %>% compute

  email_stream_write_partition(email_stream, primary_keys)
  sync_cache("email_stream", "stream")

  email_stream

}

#' @param primary_keys character vector of primary keys to use
#' @describeIn email_stream write one partition of the stream to disk
email_stream_write_partition <- function(email_stream, primary_keys) {
  # add year column for partitioning
  email_stream <- email_stream %>%
    mutate(year = year(timestamp)) %>%
    compute

  # write partitioned dataset
  write_cache(email_stream, "email_stream", "stream",
              partition = "year",
              date_column = "timestamp",
              prefer = "from",
              incremental = TRUE,
              sync = FALSE)
}

#' @importFrom dplyr arrange
#' @importFrom arrow as_arrow_table
#' @importFrom tidyselect ends_with
#' @importFrom tessilake cache_exists_any read_cache write_cache
#' @inheritDotParams tessilake::read_sql freshness
#' @describeIn email_stream appends p2 data and outputs to cache
#' @note `email_stream()` is essentially
#' ```
#' email_data(...) %>%
#'    email_data_append(...) %>%
#'    email_fix_timestamp(...) %>%
#'    email_fix_eaddress(...) %>%
#'    concat_tables(read_cache("p2_stream", "stream"), unify_schemas = TRUE) %>%
#'    email_subtype_features(...) %>%
#'    arrange(group_customer_no, timestamp)
#' ```
#' @export
email_stream <- function(...) {
  email_stream_chunk(...)
}

