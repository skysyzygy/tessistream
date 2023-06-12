#' tessi_changed_emails
#'
#' Returns list of primary emails changed since `since` date.
#'
#' @param ... other parameters passed on to `stream_from_audit`
#' @param since date after which we look for changed emails
#'
#' @importFrom dplyr lag
#' @importFrom tessilake read_sql_table
#' @importFrom lubridate now dyears
#' @return data.table of changed emails with columns `old_value`, `new_value`, and `customer_no`
tessi_changed_emails <- function(since = Sys.Date() - 7, ...) {
  . <- customer_no <- address <- eaddress_no <- timestammp <- primary_ind <- i.address <-
    address <- timestamp <- timestamp_end <- from <- to <- to2 <-
    to_last_updated_by <- to2_last_updated_by <- i.last_updated_by <- group_customer_no <- last_updated_by <- NULL

  emails <- stream_from_audit("emails", ...) %>%
    .[!is.na(customer_no) & !is.na(address)] %>%
    collect %>% setDT

  setkey(emails, eaddress_no, timestamp)

  emails[,`:=`(address = trimws(tolower(address)),
               timestamp = lubridate::force_tz(timestamp, Sys.timezone()))]

  default_time <- now() + dyears(100)
  emails[,`:=`(timestamp_end=data.table::shift(timestamp,-1,fill=default_time),
               last_updated_by=data.table::shift(last_updated_by,-1)),by="eaddress_no"]

  p <- emails[primary_ind=="Y"]
  # find the next started primary address
  p[p,to:=i.address,on=c("customer_no","timestamp_end"="timestamp"),roll=Inf]
  # or the next ended as a fallback (defaults to current)
  p[p[,.(customer_no,address,last_updated_by,
         timestamp_end=timestamp_end-.001)],to2:=i.address,on=c("customer_no","timestamp_end"),roll=Inf]

  emails <- p[,.(customer_no,
                 group_customer_no,
                 from = address,
                 to = coalesce(to,to2),
                 timestamp = timestamp_end,
                 last_updated_by)] %>%
    .[from != to & timestamp > since]

  setkey(emails, from, timestamp)
  emails[, .SD[.N], by = "from"]
}

#' p2_resolve_orphan
#'
#' Resolves orphan accounts in P2 based on the following tests:
#' 1. the email `from` exists in P2
#' 2. the email `to` is not already used in P2
#' 3. the customer # field in P2 matches a value passed in `customer_no`
#'
#' It updates the `from` email in P2 to `to`, iff #1, #2 & #3 pass;
#' or it marks the account with a tag in P2 iff #1 and #3 pass, and #2 doesn't pass
#'
#' @param from character, email in P2 that needs to be changed
#' @param to character, email that will replace the `from` email
#' @param customer_no integer vector, customer numbers; one of which must match
#' the customer # field in P2 or the update will not be run
#' @param dry_run boolean, nothing will be changed in P2 if set to `TRUE`
#' @importFrom rlang inform
#' @return `TRUE` if update is run succesfully, `FALSE` if not.
p2_resolve_orphan <- function(from = NULL, to = NULL, customer_no = NULL, dry_run = FALSE) {
  field <- value <- NULL

  customer_no_string <- paste0(customer_no, collapse = ", ")
  inform(paste("Updating", from, "to", to, "for customer #", customer_no_string))

  p2_contact_from <- p2_query_api(modify_url(
    api_url, path = "api/3/contacts",
    query = list(
      email = from,
      include = "fieldValues,contactAutomations"
    )))

  p2_contact_to <- p2_query_api(modify_url(
    api_url, path = "api/3/contacts",
    query = list(
      email = to,
      include = "fieldValues"
    )))

  tests <-

    c("From email is in P2" = !is.null(p2_contact_from$contacts) && tolower(unlist(p2_contact_from$contacts$email)) == from,
      "To email is not in P2" = is.null(p2_contact_to$contacts),
      "Customer # matches" = !is.null(p2_contact_from$fieldValues) && !is.null(p2_contact_from$fieldValues$field) &&
        unlist(p2_contact_from$fieldValues[field == 1, as.integer(value)]) %in% customer_no
    )

  # Report the result of the tests
  message <- paste(
    names(tests),":",
    cli::col_blue(c(from, to, customer_no_string))
  )
  message <- setNames(message, ifelse(tests, "i", "x"))
  inform(message)

  contact <- as.integer(p2_contact_from$contacts$id)

  # If first three pass
  if(all(tests[1:3])) {
    p2_update_email(contact, to, dry_run = dry_run)
  } else if (all(tests[c(1,3)])) {
    p2_add_tag(contact, "Orphan Account", dry_run = dry_run)
  }
}

#' p2_execute_api
#'
#' Sends a JSON `object` to a P2 API endpoint `url` using an `api_key` and returns T/F based on `success_codes`
#'
#' @param url character, endpoint url for the API
#' @param object list to be converted into JSON using `jsonlite::toJSON`
#' @param success_codes, integer vector of success codes returned by the API
#' @param method, character name of the `httr` function to call, usually `POST` or `PUT`
#' @param api_key Active Campaign API key, defaults to `keyring::key_get("P2_API")`
#' @param dry_run boolean, nothing will be changed in P2 if set to `TRUE`
#'
#' @return `TRUE` if success, `FALSE` if not
#'
#' @importFrom checkmate assert_integerish
#' @importFrom rlang sym expr warn
p2_execute_api <- function(url, object = list(), success_codes = c(200,201,202), method = "POST",
                           api_key = keyring::key_get("P2_API"), dry_run = FALSE) {
  assert_character(url,len=1)
  assert_character(method,len=1)
  assert_character(api_key,len=1)

  api_headers <- add_headers("Api-Token" = keyring::key_get("P2_API"))
  inform(c(
    "v" = paste("Executing",method,":",url),
    "*" = jsonlite::toJSON(object, auto_unbox = T)
  ))

  if (dry_run) {
    inform(c("*" = "(dry run)"))
    return(TRUE)
  }

  fun <- eval(parse(text = paste0("httr::",method)))
  response <- fun(url = url, api_headers, body = object, encode = "json")

  if (!response$status_code %in% success_codes) {
    warn(c("!" = paste(method, "to", url, "failed! Status code", response$status_code)), response = response)
    return(FALSE)
  }

  TRUE
}

#' p2_update_email
#'
#' Updates email for a contact on P2 (https://developers.activecampaign.com/reference/update-a-contact-new)
#'
#' @param contact integer P2 contact number
#' @param email character, new email for contact
#' @param dry_run boolean, nothing will be changed in P2 if set to `TRUE`
#'
#' @importFrom checkmate assert_integerish
p2_update_email <- function(contact, email, dry_run = FALSE) {
  assert_integerish(contact,len=1)
  assert_character(email,len=1)

  # Prepare to do the change via the P2 API
  p2_execute_api(
    url = modify_url(api_url, path = file.path("api/3/contacts", contact)),
    object = list(contact = list(email = email)),
    method = "PUT",
    dry_run = dry_run)

}

#' p2_add_tag
#'
#' Adds a tag to a contact on P2 (https://developers.activecampaign.com/reference/create-contact-tag)
#'
#' @param contact integer P2 contact number
#' @param tag character, string for the tag
#' @param dry_run boolean, nothing will be changed in P2 if set to `TRUE`
#'
#' @importFrom checkmate assert_integerish
p2_add_tag <- function(contact, tag, dry_run = FALSE) {
  assert_integerish(contact,len=1)
  assert_character(tag,len=1)

  tags <- p2_query_api(modify_url(api_url,
                                  path = "api/3/tags",
                                  query = list("filters[search][eq]" = tag)))

  if(is.null(tags$tags)) {
    warning(paste("Tag",shQuote(tag),"not found, not adding to contact"))
  } else {
    p2_execute_api(
      url = modify_url(api_url, path = "api/3/contactTags"),
      object = list(contactTag = list(contact = contact, tag = as.integer(tags$tags$id))),
      dry_run = dry_run)
  }

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
#' @export
p2_update_orphans <- function(freshness = 0, since = Sys.time() - 7200, test_emails = NULL) {
  . <- NULL

  customer_no_map <- collect(tessi_customer_no_map(freshness)) %>% setDT()

  tessi_changed_emails(freshness = freshness, since = since) %>%
    split(seq_len(nrow(.))) %>%
    purrr::walk(~ p2_resolve_orphan(
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
#' @param freshness difftime,	the returned data will be at least this fresh
#'
#' @return data.table of orphaned accounts
#' @export
#'
p2_orphans <- function(freshness = 0) {
  . <- status <- field <- email <- value <- id <- contact <- address <- primary_ind <- customer_no <- NULL

  p2_db_open()

  # All contacts
  p2_emails <- tbl(tessistream$p2_db, "contacts") %>% select(id,email) %>% collect %>%
    # That are currently subscribed to something
    dplyr::inner_join(tbl(tessistream$p2_db, "contactLists") %>% filter(status == "1") %>% select(contact) %>% collect,
                      by=c("id"="contact"),
                      suffix=c("",".list")) %>%
    # And have a customer # (field 1)
    dplyr::inner_join(tbl(tessistream$p2_db, "fieldValues") %>% filter(field == "1") %>% select(contact,value) %>% collect,
                      by=c("id"="contact"),
                      suffix=c("",".fieldValue")) %>%
    transmute(
      address = trimws(tolower(email)),
      customer_no = value,
      id
    ) %>% distinct %>% setDT

  tessi_emails <- read_tessi("emails", c("address", "customer_no", "primary_ind"),freshness = freshness) %>%
    filter(primary_ind=="Y") %>% collect %>% setDT() %>%
    .[,address := trimws(tolower(address))]

  p2_orphans <- p2_emails[!tessi_emails, on = "address"][!is.na(customer_no)]

}


#' p2_orphans_report
#'
#' Sends an email containing a plot of recent orphans and a spreadsheet of all orphans.
#'
#' @param freshness difftime,	the returned data will be at least this fresh
#'
#' @importFrom ggplot2 ggplot geom_histogram aes scale_fill_brewer theme_minimal
#' @importFrom dplyr case_when
#' @importFrom lubridate ddays
#' @importFrom grDevices dev.off png
#' @export
p2_orphans_report <- function(freshness = 0) {
  . <- type <- timestamp <- id <- from <- to <- customer_no.x <- expr_dt <- memb_level <-
    last_updated_by <- NULL

  p2_orphans <- p2_orphans()
  tessi_emails <- tessi_changed_emails(since = 0, freshness = freshness)

  # last change from `from`
  p2_orphan_events <- tessi_emails[p2_orphans,on=c("from"="address")]

  p2_orphan_events[,type:=case_when(trimws(last_updated_by) %in% c("popmulti","addage") ~ "web",
                                    trimws(last_updated_by) %in% c("sqladmin","sa") ~ "merge",
                                    TRUE ~ "client") %>% forcats::fct_infreq()]

  png(image_file <- tempfile(fileext = ".png"))
  ggplot(p2_orphan_events[timestamp>'2022-08-01']) +
      geom_histogram(aes(timestamp,
                         fill=type),
                     binwidth=ddays(7)) +
    scale_fill_brewer(type="qual") +
    theme_minimal() -> p



  print(p)

  dev.off()

  memberships <- read_tessi("memberships", c("expr_dt","memb_level",
                                             "customer_no")) %>%
    collect() %>% setDT() %>% .[,.SD[.N], by="customer_no"]

  p2_orphan_events <- merge(p2_orphan_events,memberships,all.x=T,by="group_customer_no")

  can_be_updated <- split(p2_orphan_events,1:nrow(p2_orphan_events)) %>%
    map(~p2_resolve_orphan(.$from, .$to, .$i.customer_no, dry_run = TRUE))


  xlsx_file <- write_xlsx(p2_orphan_events[,.(
    timestamp = as.Date(timestamp),
    "customer_#" = customer_no.x,
    p2_id = id,
    from_email = from,
    to_email = to,
    expiration_date = as.Date(expr_dt),
    member_level = memb_level,
    can_be_updated,
    change_type = type,
    last_updated_by
  )],
  xlsx_file <- tempfile(fileext = ".xlsx"))
  writeLines(paste0("<img src='",image_file,"'>"), html_file <- tempfile())

  send_email(
    subject = paste("P2 Orphan Report :",lubridate::today()),
    body = html_file,
    emails = "ssyzygy@bam.org",
    attach.files = xlsx_file,
    html = TRUE,
    file.names = paste0("p2_ophan_report_",lubridate::today(),".xlsx"),
    inline = TRUE
  )

}
