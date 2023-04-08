#' tessi_changed_emails
#'
#' Returns list of primary emails changed since `since` date.
#'
#' @param ... other parameters passed on to `stream_from_audit`
#' @param since date after which we look for changed emails
#'
#' @importFrom dplyr lag
#' @importFrom tessilake read_sql_table
#' @return data.table of changed emails with columns `old_value`, `new_value`, and `customer_no`
tessi_changed_emails <- function(since = Sys.Date() - 7, ...) {
  . <- primary_ind <- inactive <- customer_no <- status <- timestamp <- merge_dt <- event_subtype <-
    delete_id <- address <- from <- to <- NULL

  primary_emails <- stream_from_audit("emails", ...) %>%
    .[!is.na(customer_no) & !is.na(address)]

  setkey(primary_emails, eaddress_no, timestamp)

  primary_emails[,`:=`(address = trimws(tolower(address)),
                    timestamp = lubridate::force_tz(timestamp, Sys.timezone()))]

  # simple changes. Rare...for speed, just look at the last row per address
  primary_emails[, `:=`(to = address,from = c(NA, address[-.N])),
                 by = "eaddress_no"]
  # and clear it if it hasn't changed
  primary_emails[from == to, from := NA]

  # updates and changes without a log...
  # for each primary email without a from
  updates <- merge(primary_emails[primary_ind=="Y" & event_subtype=="Current" & is.na(from)],
                  # look for all non-primaries
                  primary_emails[primary_ind=="N"],
                  by="customer_no",
                  allow.cartesian = TRUE,
                  suffix=c("",".np")) %>%
    # but only take the most recent row for each non-primary address
    .[,.SD[.N],
      .SDcols=c("timestamp",
                "address.np","timestamp.np","last_updated_by.np"),
      by=c("eaddress_no","eaddress_no.np")]

  primary_emails <- merge(primary_emails,updates,all.x=T,
        by=c("eaddress_no","timestamp")) %>%
    .[!is.na(address.np) & !is.na(timestamp.np),
      `:=`(from=address.np, timestamp=timestamp.np,
           last_updated_by=last_updated_by.np)]

  primary_emails <- primary_emails[from != to & timestamp > since,
                                 .(to,from,customer_no,timestamp,event_subtype,
                                   last_updated_by)]

  setkey(primary_emails, from, timestamp)
  primary_emails[, .SD[.N], by = "from"]
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
#' @importFrom rlang inform
#' @return `TRUE` if update is run succesfully, `FALSE` if not.
p2_update_email <- function(from = NULL, to = NULL, customer_no = NULL, dry_run = FALSE) {
  field <- value <- NULL

  customer_no_string <- paste0(customer_no, collapse = ", ")
  inform(paste("Updating", from, "to", to, "for customer #", customer_no_string))

  contact_from <- p2_query_api(modify_url(
    api_url, path = "api/3/contacts",
    query = list(
      email = from,
      include = "fieldValues"
    )))

  contact_to <- p2_query_api(modify_url(
    api_url, path = "api/3/contacts",
    query = list(
      email = to,
      include = "fieldValues"
    )))

  tests <-
    c("From email" = !is.null(contact_from$contacts) && tolower(unlist(contact_from$contacts$email)) == from,
      "To email" = !is.null(contact_to$contacts),
      "Customer #" = !is.null(contact_from$fieldValues) && !is.null(contact_from$fieldValues$field) &&
        unlist(contact_from$fieldValues[field == 1, as.integer(value)]) %in% customer_no
    )

  message <- paste(
    names(tests),
    ifelse(tests, "matches", "doesn't match"),
    cli::col_blue(c(from, to, paste0(customer_no, collapse = ", ")))
  )
  tests[2] <- !tests[2]
  message <- setNames(message, ifelse(tests, "i", "x"))

  inform(message)
  if ("x" %in% names(message)) {
    return(invisible(FALSE))
  }

  url <- modify_url(api_url, path = file.path("api/3/contacts", unlist(contact_from$contacts$id)))
  obj <- list(contact = list(email = to))

  inform(c(
    "v" = "Doing it!",
    "*" = url,
    "*" = jsonlite::toJSON(obj, auto_unbox = T)
  ))

  api_headers <- add_headers("Api-Token" = keyring::key_get("P2_API"))
  if (dry_run) {
    rlang::inform(c("i" = "(dry run)"))
  } else {
    response <- httr::PUT(url, api_headers, body = obj, encode = "json")
    if (!response$status_code == 200) {
      rlang::warn(c("!" = paste("PUT to", url, "failed! Status code", response$status_code)), response = response)
    }
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
#' @param test_emails character, if set then all updates are dry_runs except for emails matching `test_emails`
#' @importFrom tessilake tessi_customer_no_map
p2_update_orphans <- function(freshness = 0, since = Sys.time() - 7200, test_emails = NULL) {
  . <- NULL

  customer_no_map <- collect(tessi_customer_no_map(freshness)) %>% setDT()

  tessi_changed_emails(freshness = freshness, since = since) %>%
    split(seq_len(nrow(.))) %>%
    purrr::walk(~ p2_update_email(
      from = .$from,
      to = .$to,
      customer_no = customer_no_map[
        merged_customer_no == .$customer_no,
        customer_no
      ],
      dry_run = !is.null(test_emails) && !grepl(test_emails,.$from)
    ))

  invisible()
}

#' p2_orphans
#'
#' Create a dataset of orphaned accounts in Prospect2 by comparing all accounts in Prospect2
#' against all accounts in Tessi. An orphan is an account that exists in P2, that originated
#' from Tessitura, but no longer matches an account with a primary email address in Tessitura.
#'
#' @return data.table of orphaned accounts
#' @export
#'
p2_orphans <- function() {
  p2_db_open()

  # All contacts
  p2_emails <- tbl(tessistream$p2_db, "contacts") %>%
    # That are currently subscribed to something
    dplyr::inner_join(tbl(tessistream$p2_db, "contactLists") %>%
                        filter(status == "1"),
               by=c("id"="contact"),
               suffix=c("",".list")) %>%
    # And have a customer # (field 1)
    dplyr::inner_join(tbl(tessistream$p2_db, "fieldValues") %>%
                       filter(field == "1"),
              by=c("id"="contact"),
              suffix=c(".contact",".fieldValue")) %>%
    transmute(
      address = tolower(email),
      customer_no = as.integer(value),
      id = as.integer(id.contact)
    ) %>% collect %>% setDT

  tessi_emails <- read_tessi("emails", c("address","customer_no", "primary_ind"),freshness=0) %>%
    dplyr::mutate(address = tolower(address)) %>%
    filter(primary_ind=="Y") %>% collect %>% setDT()

  p2_orphans <- p2_emails[!tessi_emails, on = "address"][!is.na(customer_no)]

}

p2_orphans_report <- function() {

  p2_orphans <- p2_orphans()
  tessi_emails <- tessi_changed_emails(since = 0)

  # last change from `from`
  p2_orphan_events <- tessi_emails[p2_orphans,on=c("from"="address")]

  p2_orphan_events %>% .[timestamp>'2022-10-01'] %>% .[,
                         type:=forcats::fct_lump(last_updated_by,2) %>%
                           forcats::fct_inorder() %>%
                           forcats::fct_relabel(trimws) %>%
                           forcats::fct_recode(web="addage",
                                               merge="sqladmin",
                                               client="Other")
                         ] %>%
    ggplot() + geom_histogram(aes(timestamp,
                                  fill=type),
                              binwidth=lubridate::ddays(7)) +
    scale_fill_brewer(type="qual")

  p2_orphan_events[timestamp<'2022-10-01']

  # latest match
  last_match <- tessi_emails[primary_ind == "Y"][, last_timestamp := timestamp][p2_orphans, on = c("address", "timestamp" = "updated_timestamp"), roll = Inf][!is.na(eaddress_no)]
  un_match <- p2_orphans[!last_match, , on = "address"]
  last_match2 <- tessi_emails[, last_timestamp := timestamp][un_match, on = c("address", "timestamp" = "updated_timestamp"), roll = Inf][!is.na(eaddress_no)]
  last_match <- rbind(last_match, last_match2)
  un_match <- p2_orphans[!last_match, , on = "address"]

  current_email <- tessi_emails[primary_ind == "Y" & event_subtype == "Current"][last_match, on = c("customer_no" = "customer_no")][!is.na(eaddress_no)]
  # updates
  updates <- current_email[timestamp > "2022-11-25"]

  ggplot(updates) +
    geom_histogram(aes(timestamp, fill = customer_no != i.customer_no))
  ggplot(current_email) +
    geom_histogram(aes(timestamp, fill = customer_no != i.customer_no))
  ggplot(updates) +
    geom_histogram(aes(timestamp, fill = last_updated_by))

  # customer_no changed (merged or household change)
  merges <- last_match[customer_no != i.customer_no]
  # updated email
  updated_emails <- last_match[customer_no == i.customer_no & primary_ind == "Y"]

  m <- tessilake::read_sql_table("T_MERGED") %>%
    collect() %>%
    setDT()
  m <- m[status == "S"][merges, on = c("kept_id" = "customer_no", "merge_dt" = "timestamp"), roll = "nearest"]

  # true merges
  m[!is.na(request_dt) & request_dt >= "2022-08-01"]
  # household operation
  m[is.na(request_dt) | request_dt < "2022-08-01"]

  u <- tessi_emails[primary_ind == "Y"][, next_timestamp := timestamp][!updated_emails, on = c("address")][updated_emails, .(
    customer_no, eaddress_no, next_timestamp, address, i.address, i.last_timestamp, last_updated_by
  ), on = c("customer_no", "timestamp"), roll = "nearest"]
  # true updates
  u[!is.na(eaddress_no) & next_timestamp >= "2022-08-01"]
  u[is.na(eaddress_no) | next_timestamp < "2022-08-01"]
}
