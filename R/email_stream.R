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

  ### Promotion responses

  promotion_responses = read_tessi("promotion_responses", ...) %>%
    filter(response_dt >= from_date & response_dt < to_date) %>%
    transmute(group_customer_no,customer_no,
              response, timestamp = response_dt,
              source_no, url_no) %>%
    compute

  ### Promotions

  promotions = read_tessi("promotions", ...) %>%
    filter(promote_dt >= from_date & promote_dt < to_date &
            media_type == 3) %>%
    select(group_customer_no,customer_no,eaddress,
           promote_dt,appeal_no,campaign_no,source_no) %>%
    compute

  ### Promotion responses by source (because promotion timestamps may be modified by source)

  promotion_responses2 = read_tessi("promotion_responses", ...) %>%
    filter(source_no %in% c(promotions$source_no, promotion_responses$source_no) &
             !(response_dt >= from_date & response_dt < to_date)) %>%
    transmute(group_customer_no,customer_no,
              response, timestamp = response_dt,
              source_no, url_no) %>%
    compute

  ### Promotions by source (because promotion timestamps may be modified by source)

  promotions2 = read_tessi("promotions", ...) %>%
    filter(source_no %in% c(promotions$source_no, promotion_responses$source_no) &
             !(promote_dt >= from_date & promote_dt < to_date) &
             media_type == 3) %>%
    select(group_customer_no,customer_no,eaddress,
           promote_dt,appeal_no,campaign_no,source_no) %>%
    compute

  ### Assemble email_stream

  promotions = concat_tables(promotions,promotions2)
  promotion_responses = concat_tables(promotion_responses,promotion_responses2)

  promotion_responses <- promotion_responses %>%
    left_join(summarise(promotions,
                        appeal_no = max(appeal_no),
                        eaddress = max(eaddress),
                        campaign_no = max(campaign_no),
                        .by = source_no)) %>%
    compute


  email_stream <- concat_tables(promotions, promotion_responses, unify_schemas = TRUE) %>%
    compute

  email_stream$metadata$r$attributes$primary_keys <- NULL
  email_stream

}


#' @describeIn email_stream adds descriptive campaign/appeal/source information from Tessitura
#' @param email_stream email data from previous step
email_data_append <- function(email_stream, ...) {

  source_no <- source_desc <- acq_dt <- event_subtype <- NULL

  ### Response descriptions

  responses = read_sql("select email_response response,
                        description event_subtype from TR_EMAIL_RESPONSE_CODE erc
                        join TR_RESPONSE r on
                        erc.promo_response=r.id", ...) %>%
    mutate(event_subtype = case_when(grepl("open",event_subtype,ignore.case=T) ~ "Open",
                                     grepl("click",event_subtype,ignore.case=T) ~ "Click",
                                     grepl("unsub",event_subtype,ignore.case=T) ~ "Unsubscribe",
                                     grepl("soft",event_subtype,ignore.case=T) ~ "Soft Bounce",
                                     grepl("hard",event_subtype,ignore.case=T) ~ "Hard Bounce",
                                     TRUE ~ event_subtype))

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

  timestamp <- source_no <- first_response_dt <- acq_dt <- promote_dt <- timestamp_id <- NULL

  ### Fix timestamp for sends/promotions

  # recalculate timestamps because promotion often happens long before the actual send
  # NOTE: Have to bring this into R because aggregation by group not implemented in Arrow
  first_response <- email_stream %>% filter(!is.na(timestamp)) %>%
    summarise(first_response_dt = min(timestamp, na.rm = TRUE), .by = source_no) %>%
    collect

  email_stream <- email_stream %>% left_join(first_response, by = "source_no") %>%
    # 1. use first recorded response for source
    # TODO: 2. use acq_dt from the source code because this is sometimes set manually as a replacement for acq_dt
    # 3. use promote_dt
    mutate(timestamp = coalesce(timestamp,ifelse(!is.na(first_response_dt) & abs(first_response_dt - promote_dt) < lubridate::ddays(30),
                                                 first_response_dt,
                                                 promote_dt)),
    # Arrow uses int64 for timestamps; R uses double precision floating points.
    # to avoid precision loss and failed joins, create a timestamp_id for joins
           timestamp_id = arrow:::cast(timestamp, arrow::int64())) %>%
    compute
}

#' @importFrom dplyr select collect compute
#' @importFrom arrow as_arrow_table
#' @describeIn email_stream fills in email address based on time of send and the current email address for the customer,
#' using email data from [stream_from_audit]
email_fix_eaddress <- function(email_stream) {
  . <- address <- primary_ind <- customer_no <- group_customer_no <- timestamp <- timestamp_id <-
    eaddress <- i.address <- domain <- NULL

  ### Email addresses
  emails <- stream_from_audit("emails") %>%
    .[!is.na(address) & address>0 & primary_ind=="Y"]

  # Match email address based on customer_no (or group_customer_no) and send date using a rolling join
  # NOTE: Have to bring this into R because inequality joins and rolling joins not implemented in Arrow
  email_matches <- email_stream %>%
    transmute(customer_no, group_customer_no, timestamp, timestamp_id, eaddress) %>%
    collect %>% setDT %>% setkey %>%
    stream_debounce("customer_no", "timestamp_id")

  email_matches <- emails[,.(customer_no,timestamp,address)][
    email_matches,on=c("customer_no","timestamp"),roll=T]
  email_matches <- emails[,.(group_customer_no,timestamp,address)][
    email_matches,on=c("group_customer_no","timestamp"),roll=T]

  # clean email address and extract domain
  email_matches[,eaddress := stringr::str_trim(tolower(coalesce(eaddress, address, i.address)))]
  emails_cleaned <- data.table(eaddress = unique(email_matches$eaddress)) %>%
    .[,domain := stringr::str_replace(eaddress, ".+@", "")]
  setleftjoin(email_matches, emails_cleaned, by = "eaddress")

  # provide event_type and event_subtype and remove columns
  email_stream <- email_stream %>% select(-eaddress) %>% collect %>%
    left_join(email_matches[,.(customer_no,timestamp_id,eaddress,domain)],
              by = c("customer_no","timestamp_id")) %>%
    as_arrow_table

}


#' @importFrom dplyr select collect compute
#' @importFrom tidyr any_of
#' @importFrom checkmate assert_multi_class
#' @describeIn email_stream sets multiple `event_subtype == "open"` as `"forward"` and builds windowed features
#' for each `event_subtype`
email_subtype_features <- function(email_stream) {
  group_customer_no <- timestamp <- timestamp_id <- source_no <- event_subtype <- . <- NULL

  # must be an arrow query in order to extract customer history from the full dataset
  assert_multi_class(email_stream, c("arrow_dplyr_query","ArrowTabular"))

  if(nrow(email_stream) == 0) {
    return(email_stream)
  }

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
                              pattern = "(count|min|max)$")
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

    cols = grep(gsub("\\s","_",tolower(subtype)), feature_cols, value = T, fixed = TRUE) %>% unique

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
                   by=c("group_customer_no")] %>%
      .[,(i_cols) := NULL]

  }

  # Fill down, respecting customer boundaries
  setnafill(email_subtypes,
            type = "locf",
            cols = feature_cols,
            by = "group_customer_no")

  # remove added customer history and debounce so it can be left joined
  email_subtypes <- email_subtypes[timestamp >= min_timestamp] %>%
    setkey(group_customer_no, timestamp) %>%
    stream_debounce(c("group_customer_no","timestamp_id"))

  email_stream %>% select(!any_of(feature_cols)) %>%
    left_join(email_subtypes %>% select(all_of(c("group_customer_no","timestamp_id",feature_cols))),
              by=c("group_customer_no", "timestamp_id")) %>%
    compute

}

#' @describeIn email_stream produce one chunk of the base email_stream (without p2_stream or subtype features)
email_stream_base <- function(from_date = as.POSIXct("1900-01-01"), to_date = now(), ...) {

  timestamp <- group_customer_no <- customer_no <- timestamp_id <- event_subtype <-
    source_no <- appeal_no <- campaign_no <- source_desc <- extraction_desc <-
    response <- url_no <- eaddress <- domain <- NULL

  email_stream <- email_data(from_date = from_date, to_date = to_date, ...)

  if (nrow(email_stream) == 0) {
    return(arrow::arrow_table(group_customer_no=integer(0)))
  }

  email_stream %>% email_data_append(...) %>% email_fix_timestamp %>%
  filter(timestamp >= from_date & timestamp < to_date) %>% email_fix_eaddress %>%
  transmute(group_customer_no = as.integer(group_customer_no), customer_no,
            timestamp, timestamp_id, event_type = "Email", event_subtype,
            source_no, appeal_no, campaign_no, source_desc, extraction_desc,
            response, url_no, email = eaddress, domain) %>% compute
}


#' @importFrom tessilake cache_exists_any read_cache
#' @importFrom checkmate assert_posixct
#' @inheritDotParams tessilake::read_sql freshness
#' @describeIn email_stream produce one chunk of email_stream between `from_date` and `to_date`
#' @param from_date earliest date/time for which data will be returned
#' @param to_date latest date/time for which data will be returned
email_stream_chunk <- function(from_date = as.POSIXct("1900-01-01"), to_date = now(), ...) {
  campaignid <- timestamp <- NULL

  assert_posixct(c(from_date, to_date), sorted = TRUE)

  email_stream <- email_stream_base(from_date = from_date, to_date = to_date)

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

  email_stream_write_partition(email_stream)

  if(cache_exists_any("email_stream","stream")) {
    email_stream <- read_cache("email_stream","stream") %>%
      filter(timestamp >= from_date & timestamp < to_date) %>%
      email_subtype_features %>% compute
  }

  suppressWarnings(email_stream_write_partition(email_stream))

  email_stream

}

#' @describeIn email_stream write one partition of the stream to disk
email_stream_write_partition <- function(email_stream) {
  timestamp <- NULL

  if(nrow(email_stream) == 0) {
    return(invisible())
  }

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

#' @describeIn email_stream appends p2 data and outputs to cache
#' @note `email_stream()` is essentially
#' ```
#' email_data(...) %>%
#'    email_data_append(...) %>%
#'    email_fix_timestamp(...) %>%
#'    email_fix_eaddress(...) %>%
#'    concat_tables(read_cache("p2_stream", "stream"), unify_schemas = TRUE) %>%
#'    email_subtype_features(...)
#' ```
#' @export
email_stream <- function(from_date = as.POSIXct("1900-01-01"), to_date = now(), ...) {
  assert_posixct(c(from_date, to_date), sorted = TRUE)

  dates <- seq(from_date,to_date,by="year")

  for(i in seq_len(length(dates)-1)) {
    email_stream_chunk(from_date = dates[i], to_date = dates[i+1])
  }
  email_stream_chunk(from_date = dates[length(dates)], to_date = to_date)

  sync_cache("email_stream", "stream", partition = "year")
}

