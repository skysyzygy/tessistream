
library(progressr)

progressr::handlers(list(
  progressr::handler_progress(
    format   = ":spin :current/:total (:message) [:bar] :percent in :elapsed ETA: :eta",
    width    = 60,
    complete = "+"
  )
))


api_url <- "https://brooklynacademyofmusic.api-us1.com"

#' p2_query_api
#'
#' Parallel load from P2/Active Campaign API at `url` with key `api_key`. Loads pages of 100 records until it reaches the total.
#'
#' @param url Active Campaign API url to query
#' @param api_key Active Campaign API key, defaults to `keyring::key_get("P2_API")`
#'
#' @return JSON object as a list
#' @importFrom httr modify_url GET content add_headers
#' @importFrom progressr progressor
#' @importFrom future availableWorkers
p2_query_api <- function(url, api_key = keyring::key_get("P2_API")) {

  api_headers <- add_headers("Api-Token"=api_key)

  total <- modify_url(url,query=list(limit=1)) %>% GET(api_headers) %>% content() %>% {.$meta$total %>% as.integer}
  p <- progressor(total)

  jobs <- data.table(offset=seq(0,total,by=100))
  jobs <- jobs[,length:=c(offset[-1],total)-offset][length>0]

 # environment(p2_json_to_datatable) <- sys.frame()

  furrr::future_map2(jobs$offset, jobs$length,~{
    res <- GET(modify_url(url,query=list(offset=.x,limit=.y)),api_headers) %>% content() %>%
      map(p2_json_to_datatable)
    p(amount=100)
    res
  }) %>% p2_combine_jsons
}

#' p2_combine_jsons
#'
#' Combine `JSON` objects returned by the Active Campaign API, concatenating each element with the same name.
#'
#' @param jsons list of list of `JSON` objects as data.tables, as returned by p2_json_to_datatable
#'
#' @return single `JSON` object as a list
#' @importFrom purrr map
p2_combine_jsons <- function(jsons) {
  # combine results
  names <- do.call(c,map(jsons,names)) %>% unique
  map(setNames(names,names), ~{name=.;rbindlist(map(jsons,name),fill = TRUE)})
}

#' p2_get_contacts
#'
#' Load all contacts in P2 using Active Campaign API
#'
#' @param start_dt POSIXct, only contacts that have been created or updated since this date will be returned. (Note: side-loaded tables may have earlier entries.)
#'
#' @return single `JSON` object of all contacts
#' @importFrom httr modify_url
p2_get_contacts <- function(start_dt = as.POSIXct("1970-01-01 00:00:00")) {
  p2_query_api(modify_url(api_url,path="api/3/contacts",
                          query=list("filters[updated_after]"=format(start_dt,"%FT%T"))))
}


#' p2_get_activities
#'
#' @param contacts vector of contact ids as integers or strings
#' @param start_dt POSIXct, only activities that have been created or updated since this date will be returned. (Note: side-loaded tables may have earlier entries.)
#'
#' @return `JSON` object of all email activities
#' @importFrom httr modify_url
#' @importFrom furrr future_map
p2_get_activities <- function(contacts, start_dt = as.POSIXct("1970-01-01 00:00:00")) {

  # p <- progressor(length(contacts))
  #
  # furrr::future_map(contacts,~{
    ret <- p2_query_api(modify_url(api_url,path="api/3/activities",
                          query=list(type=paste("log","link-data","update","reply","forward",
                                            "sms-log","sms-reply","sms-unsub",
                                            "tracking-log",sep=","),
                                     # contact=.x,
                                     after=format(start_dt,"%F"))))
  #   p()
  #   ret
  # }) %>% p2_combine_jsons
}

#' p2_get_emails
#'
#' @param start_dt POSIXct, only emails that have been created or updated since this date will be returned. (Note: side-loaded tables may have earlier entries.)
#'
#' @return `JSON` object of all email activities
#' @importFrom httr modify_url
#' @importFrom furrr future_map
p2_get_emails <- function(start_dt = as.POSIXct("1970-01-01 00:00:00")) {


  p2_query_api(modify_url(api_url,path="api/3/emailActivities"))

}

#' p2_json_to_datatable
#'
#' Convert P2 JSON objects to a data table
#'
#' @param json list of P2 JSON objects
#' @importFrom purrr map discard keep
#' @importFrom data.table as.data.table
p2_json_to_datatable <- function(json) {
  keep(json,is.list) %>%
    do.call(what=rbind) %>%
    as.data.table()
}


if(FALSE) {

  with_progress(p2_query_api(modify_url(api_url,path="api/3/links?include=linkData"))) -> links #clicks/opens (linkData holds the logs)
  with_progress(p2_query_api(modify_url(api_url,path="api/3/logs"))) -> emails #sends
  with_progress(p2_query_api(modify_url(api_url,path="api/3/campaigns"))) -> campaigns
  with_progress(p2_query_api(modify_url(api_url,path="api/3/messages"))) -> messages
  with_progress(p2_query_api(modify_url(api_url,path="api/3/contacts"))) -> contacts

  save.image()


emails <- read_tessi("emails") %>% dplyr::collect() %>% setDT
inactive_emails <- emails[primary_ind=="N"][!emails[primary_ind=="Y"],on="address"]

p2_emails <- purrr::map_chr(contacts,"email") %>% unique()
p2_orphan <- p2_emails[!trimws(tolower(p2_emails)) %in% emails[primary_ind=="Y",trimws(tolower(address))]]

# Add "Orphan account" tag and zero out membership data
p2_import <- data.frame(Email=p2_orphan,
                        Tags="Orphan Account",
                        MEMBER_LEVEL=NA,
                        RECOGNITION_AMOUNT=NA,
                        INITIATION_DATE=NA,
                        EXPIRATION_DATE=NA,
                        CONSTITUENCY_STRING_WITH_AFFILIATES=NA,
                        BENEFIT_PROVIDER=NA,
                        CURRENT_STATUS=NA,
                        CUSTOMER_LAST_GIFT_DT=NA,
                        CUSTOMER_LAST_TICKET_DT=NA)

write.csv(p2_import,"p2_import.csv",row.names = F, na = "")

save.image()
}
