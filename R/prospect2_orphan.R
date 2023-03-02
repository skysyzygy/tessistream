

#' tessi_changed_emails
#'
#' Returns list of primary emails changed since `since` date.
#'
#' @param ... other parameters passsed on to `stream_from_audit`
#' @param since date after which we look for changed emails
#'
#' @importFrom dplyr lag
#' @return data.table of changed emails with columns `old_value`, `new_value`, and `customer_no`
tessi_changed_emails <- function(since = Sys.Date() - 7, ...) {
  primary_emails <- stream_from_audit("emails", ...)[primary_ind=="Y" & inactive=="N" & !is.na(customer_no)]
  setkey(primary_emails,customer_no,timestamp)
  primary_emails <- primary_emails[,.(to=address,from=c(NA,address[-.N]),timestamp),by="customer_no"] %>%
    .[,`:=`(from=tolower(from),
            to=tolower(to))] %>%
    .[from != to & timestamp > since]
  setkey(primary_emails,from,timestamp)
  primary_emails[,.SD[.N],by="from"]
}

p2_update_email <- function(from = NULL, to = NULL, customer_no = NULL, ...) {
  rlang::inform(paste("Updating",from,"to",to,"for customer #",customer_no))

  contact_from <- p2_query_api(modify_url(api_url,path="api/3/contacts",query=list(email=from,
                                                                              include="fieldValues")))
  contact_to <- p2_query_api(modify_url(api_url,path="api/3/contacts",query=list(email=to,
                                                                              include="fieldValues")))
  tests <-
    c("From email" = !is.null(contact_from$contacts) && tolower(unlist(contact_from$contacts$email)) == from,
      "To email" = !is.null(contact_to$contacts),
      "Customer #" = !is.null(contact_from$fieldValues) && !is.null(contact_from$fieldValues$field) &&
        unlist(contact_from$fieldValues[field==1 ,as.integer(value)]) == customer_no)

  message <- paste(names(tests),ifelse(tests,"matches","doesn't match"),cli::col_blue(c(from,to,customer_no)))
  tests[2] = !tests[2]
  message <- setNames(message,ifelse(tests,"i","x"))

  rlang::inform(message)
  if( "x" %in% names(message))
    return()

  url = modify_url(api_url,path=file.path("api/3/contacts",unlist(contact_from$contacts$id)))
  obj = list(contact=list(email=to))

  rlang::inform(c(v = "Doing it!",
                  "*" = url,
                  "*" = jsonlite::toJSON(obj,auto_unbox = T)))

  api_headers <- add_headers("Api-Token" = keyring::key_get("P2_API"))
  response <- httr::PUT(url,api_headers,body=obj,encode="json")

  if(!response$status_code == 200)
     rlang::warn(c("!" = paste("PUT to",url,"failed! Status code",response$status_code)),response = response)
}

p2_update_orphans <- function(freshness = 0, since = Sys.Date() - 1) {
  tessi_changed_emails <- tessi_changed_emails(freshness = freshness, since = since)

  split(tessi_changed_emails,seq(nrow(tessi_changed_emails))) %>%
    purrr::walk(~p2_update_email(from=.$from,
                                to=.$to,
                                customer_no=.$customer_no))
}
