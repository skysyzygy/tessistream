

#' tessi_changed_emails
#'
#' Returns list of primary emails changed since `since` date.
#'
#' @param ... other parameters passed on to `stream_from_audit`
#' @param since date after which we look for changed emails
#'
#' @importFrom dplyr lag
#' @return data.table of changed emails with columns `old_value`, `new_value`, and `customer_no`
tessi_changed_emails <- function(since = Sys.Date() - 7, ...) {

  primary_emails <- stream_from_audit("emails", ...)[primary_ind=="Y" & inactive=="N" & !is.na(customer_no)]
  setkey(primary_emails,customer_no,timestamp)

  primary_emails <- primary_emails[,.(
    to=tolower(address),
    from=c(NA,tolower(address)[-.N]),
    timestamp = lubridate::force_tz(timestamp,Sys.timezone()),
    event_subtype),
    by="customer_no"] %>%
    .[from != to & timestamp > since & event_subtype == "Current"]

  setkey(primary_emails,from,timestamp)
  primary_emails[,.SD[.N],by="from"]

}

#' p2_update_email
#'
#' Updates the `from` email in P2 to `to`. The update will fail if:
#' * the email `from` does not exist in P2
#' * the email `to` is already used in P2
#' * the customer # field in P2 does not match a value passed to `customer_no`
#'
#' @param from character, email in P2 that needs to be changed
#' @param to character, email that will replace the `from` email
#' @param customer_no integer vector, customer numbers; one of which must match
#' the customer # field in P2 or the update will not be run
#' @param dry_run boolean, nothing will be changed in P2 if set to `TRUE`
#'
#' @return `TRUE` if update is run succesfully, `FALSE` if not.
p2_update_email <- function(from = NULL, to = NULL, customer_no = NULL, dry_run = FALSE) {
  customer_no_string = paste0(customer_no,collapse=", ")
  rlang::inform(paste("Updating",from,"to",to,"for customer #",customer_no_string))

  contact_from <- p2_query_api(modify_url(api_url,path="api/3/contacts",query=list(email=from,
                                                                              include="fieldValues")))
  contact_to <- p2_query_api(modify_url(api_url,path="api/3/contacts",query=list(email=to,
                                                                              include="fieldValues")))
  tests <-
    c("From email" = !is.null(contact_from$contacts) && tolower(unlist(contact_from$contacts$email)) == from,
      "To email" = !is.null(contact_to$contacts),
      "Customer #" = !is.null(contact_from$fieldValues) && !is.null(contact_from$fieldValues$field) &&
        unlist(contact_from$fieldValues[field==1 ,as.integer(value)]) %in% customer_no)

  message <- paste(names(tests),
                   ifelse(tests,"matches","doesn't match"),
                   cli::col_blue(c(from,to,paste0(customer_no,collapse=", "))))
  tests[2] = !tests[2]
  message <- setNames(message,ifelse(tests,"i","x"))

  rlang::inform(message)
  if( "x" %in% names(message))
    return(invisible(FALSE))

  url = modify_url(api_url,path=file.path("api/3/contacts",unlist(contact_from$contacts$id)))
  obj = list(contact=list(email=to))

  rlang::inform(c("v" = "Doing it!",
                  "*" = url,
                  "*" = jsonlite::toJSON(obj,auto_unbox = T)))

  api_headers <- add_headers("Api-Token" = keyring::key_get("P2_API"))
  if(dry_run) {
    rlang::inform(c("i"="(dry run)"))
  } else {
    response <- httr::PUT(url,api_headers,body=obj,encode="json")
    if(!response$status_code == 200)
      rlang::warn(c("!" = paste("PUT to",url,"failed! Status code",response$status_code)),response = response)
  }

  invisible(TRUE)
}

#' p2_update_orphans
#'
#' Run `p2_update_email` for all emails changed since `since` using Tessitura data at least as
#'  fresh as `freshness`
#'
#' @param freshness datediff, passed on to `tessilake` functions, defaults to 0 (refresh all data)
#' @param since datetime, passed on to `tessi_changed_emails`, defaults to the last two hours.
#'
p2_update_orphans <- function(freshness = 0, since = Sys.time()-7200) {
  customer_no_map <- collect(tessilake::tessi_customer_no_map(freshness)) %>% setDT

  tessi_changed_emails(freshness = freshness, since = since) %>%
    split(seq_len(nrow(.))) %>%
    purrr::walk(~p2_update_email(from=.$from,
                                to=.$to,
                                customer_no=customer_no_map[merged_customer_no == .$customer_no,
                                                            customer_no],
                dry_run = !grepl("@bam.org$",.$from)))

  invisible()
}

p2_orphans <- function() {

  p2_db_open()

  contacts <- tbl(tessistream$p2_db, "contacts") %>%
    transmute(created_by,
           created_timestamp = unixepoch(cdate),
           updated_by,
           updated_timestamp = unixepoch(udate),
           address = tolower(email),
           id = as.integer(id)) %>%
    collect %>% setDT

  field_values <- tbl(tessistream$p2_db, "fieldValues") %>%
    transmute(created_by,
              created_timestamp = unixepoch(cdate),
              updated_by,
              updated_timestamp = unixepoch(udate),
              customer_no = as.integer(value),
              id = as.integer(contact)) %>%
    collect %>% setDT

  contact_lists <- tbl(tessistream$p2_db, "contactLists") %>%
    filter(status=="1") %>%
    transmute(list = as.integer(list),
              id = as.integer(contact)) %>%
    collect %>% setDT

  p2_emails <- merge(contacts,field_values,by="id",all.x=T,suffix=c("",".customer_no")) %>% dplyr::semi_join(contact_lists,by="id")
  tessi_emails <- stream_from_audit("emails")
  tessi_emails[!is.na(`NA`) & is.na(address), address:=`NA`]
  tessi_emails[,address:=tolower(address)]

  p2_orphans <- p2_emails[!tessi_emails[event_subtype=="Current" & primary_ind=="Y"],on="address"][!is.na(customer_no)]

  # latest match
  last_match <- tessi_emails[primary_ind=="Y"][,last_timestamp:=timestamp][p2_orphans,on=c("address","timestamp"="updated_timestamp"),roll=Inf][!is.na(eaddress_no)]
  un_match <- p2_orphans[!last_match,,on="address"]
  last_match2 <- tessi_emails[,last_timestamp:=timestamp][un_match,on=c("address","timestamp"="updated_timestamp"),roll=Inf][!is.na(eaddress_no)]
  last_match = rbind(last_match,last_match2)
  un_match <- p2_orphans[!last_match,,on="address"]

  current_email <- tessi_emails[primary_ind=="Y" & event_subtype=="Current"][last_match,on=c("customer_no"="customer_no")][!is.na(eaddress_no)]
  # updates
  updates <- current_email[timestamp>'2022-11-25']

  ggplot(updates) + geom_histogram(aes(timestamp,fill=customer_no!=i.customer_no))
  ggplot(current_email) + geom_histogram(aes(timestamp,fill=customer_no!=i.customer_no))
  ggplot(updates) + geom_histogram(aes(timestamp,fill=last_updated_by))

  # customer_no changed (merged or household change)
  merges <- last_match[customer_no!=i.customer_no]
  # updated email
  updated_emails <- last_match[customer_no==i.customer_no & primary_ind=="Y"]

  m <- tessilake::read_sql_table("T_MERGED") %>% collect %>% setDT
  m <- m[status=="S"][merges,on=c("kept_id"="customer_no","merge_dt"="timestamp"),roll="nearest"]

  # true merges
  m[!is.na(request_dt) & request_dt>='2022-08-01']
  # household operation
  m[is.na(request_dt) | request_dt<'2022-08-01']

  u <- tessi_emails[primary_ind=="Y"][,next_timestamp:=timestamp][!updated_emails,on=c("address")][updated_emails,.(
    customer_no,eaddress_no,next_timestamp,address,i.address,i.last_timestamp,last_updated_by),on=c("customer_no","timestamp"),roll="nearest"]
  # true updates
  u[!is.na(eaddress_no) & next_timestamp>='2022-08-01']
  u[is.na(eaddress_no) | next_timestamp<'2022-08-01']
}
