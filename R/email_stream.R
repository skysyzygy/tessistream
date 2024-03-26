#' @title email_stream
#' @description
#' * group_customer_no
#' * timestamp : date of email event
#' * event_type : Email
#' * event_subtype : Open|Click|Unsubscribe|Hard Bounce|Forward
#' -* event_subtype2 : Onsale|Newsletter|Film|Fundraising-
#' * campaign_no
#' * appeal_no
#' * source_no
#' * extraction_desc
#' * source_desc
#' * address
#' * domain
#' * email\[subtype\]TimestampMin
#' * email\[subtype\]TimestampMax
#' -* email\[subtype\]\[subtype2\]Count-
#' @name email_stream
NULL


#' @describeIn email_stream load Tessitura email send/response data
#' @importFrom tessilake read_sql read_tessi
#' @importFrom dplyr filter compute collect
#' @importFrom data.table tstrsplit
#' @inheritDotParams tessilake::read_sql freshness
email_data <- function(...) {

  ### Promotions

  promotions = read_tessi("promotions", ...) %>%
    filter(media_type == 3) %>%
    select(group_customer_no,customer_no,eaddress,
           promote_dt,appeal_no,campaign_no,source_no) %>%
    compute

  ### Promotion responses

  promotion_responses = read_tessi("promotion_responses", ...) %>%
    # Get rid of erroneous 1970 responses !!
    filter(response_dt > as.Date('2000-01-01')) %>%
    transmute(group_customer_no,customer_no,
              response, timestamp = response_dt,
              source_no, url_no) %>%
    compute

  ### Assemble email_stream

  email_stream <- arrow::concat_tables(promotions, promotion_responses, unify_schemas = TRUE)

}

#' @importFrom dplyr mutate
email_data_append <- function(email_stream, ...) {

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
                          source_no is not null")

  email_stream <- email_stream %>%
    left_join(responses, by = "response") %>%
    left_join(sources, by = "source_no") %>%
    left_join(extractions, by = "source_no") %>%
    compute

  ### Fix timestamp for sends/promotions

  # recalculate timestamps because promotion often happens long before the actual send
  # NOTE: Have to bring this into R because window aggregation not implemented in Arrow
  first_response <- email_stream %>% dplyr::summarise(first_response_dt = min(timestamp, na.rm = TRUE),
                                                      .by = source_no) %>%
    collect

  email_stream <- email_stream %>% left_join(first_response, by = "source_no") %>%
    # 1. use first recorded response for source
    # 2. use acq_dt from the source code because this is sometimes set manually as a replacement for acq_dt
    # 3. use promote_dt
    mutate(timestamp = coalesce(timestamp,first_response_dt,acq_dt,promote_dt))

  ### Email addresses

  emails <- stream_from_audit("emails") %>%
    .[!is.na(address) & address>0 & primary_ind=="Y"]

  # Match email address based on customer_no (or group_customer_no) and send date using a rolling join
  # NOTE: Have to bring this into R because inequality joins and rolling joins not implemented in Arrow
  email_matches <- email_stream %>% select(customer_no, group_customer_no, timestamp, eaddress) %>%
    collect %>% setDT %>% .[,row:=.I]

  email_matches <- emails[,.(customer_no,timestamp,address)][email_matches,on=c("customer_no","timestamp"),roll=T]
  email_matches <- emails[,.(group_customer_no,timestamp,address)][email_matches,on=c("group_customer_no","timestamp"),roll=T]
  # clean email address and extract domain
  email_matches[,eaddress := stringr::str_trim(tolower(coalesce(eaddress, address, i.address)))] %>%
    .[,domain := stringr::str_replace(eaddress, ".+@", "")]

  # Fill up earliest email address
  # data.table::setorder(email_matches, customer_no, timestamp) %>%
  #   setnafill("nocb", cols = c("eaddress","domain"), by = "customer_no") %>%
  #   setkey(row)

  # provide event_type and event_subtype and remove columns
  email_stream <- email_stream %>% select(-eaddress) %>% compute %>%
    cbind(eaddress = email_matches$eaddress,
          domain = email_matches$domain) %>%
    transmute(group_customer_no,customer_no,timestamp,
                                             event_type = "Email",
                                             event_subtype = coalesce(event_subtype,"Send"),
                                             source_no, appeal_no, campaign_no, source_desc, extraction_desc,
                                             response, url_no, eaddress, domain) %>%
    compute

}

#' @importFrom tessistream p2_stream
#' @importFrom dplyr arrange
email_stream <- function() {

  email_stream <- email_data() %>% email_data_append() #%>%
    #arrow::concat_tables(p2_stream(),unify_schema = TRUE)

  # Build time-varying customer data
  email_stream <- arrange(email_stream, group_customer_no, timestamp)
  email_subtypes <- email_stream %>%
    select(group_customer_no, timestamp, source_no, event_subtype) %>%
    collect %>% setDT

  # Label multiple opens of a source as a forward
  email_subtypes[email_subtypes[grepl("open",event_subtype,ignore.case=T),
                                tail(.I,-1),
                                by=c("group_customer_no","source_no")]$V1,
                 event_subtype:="Forward"]

  for( subtype in unique(email_subtypes$event_subtype) ) {
    # email[subtype]TimestampMin
    # email[subtype]TimestampMax
    # email[subtype]Count

    cols = paste("email",gsub("\\s","_",tolower(subtype)),c("timestamp_min", "timestamp_max", "count"),sep="_")
    email_subtypes[event_subtype==subtype,
                   (cols) := list(min(timestamp),timestamp,seq_len(.N)),
                   by=group_customer_no]
    #Fill down, respecting customer boundaries
    setnafill(email_subtypes,
              type = "locf",
              cols = cols,
              by = "group_customer_no")

  }

  email_stream <- cbind(email_stream %>% select(-event_subtype) %>% compute,
                        email_subtypes[,.SD,.SDcols = -c("timestamp", "group_customer_no", "source_no")])

  write_cache(email_stream, "email_stream", "stream")

  email_stream

}


