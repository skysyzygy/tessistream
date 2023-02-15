api_url <- "https://brooklynacademyofmusic.api-us1.com"

#' p2_query_api
#'
#' Parallel load from P2/Active Campaign API at `url` with key `api_key`. Loads pages of 100 records until it reaches the total.
#'
#' @param url Active Campaign API url to query
#' @param api_key Active Campaign API key, defaults to `keyring::key_get("P2_API")`
#' @param offset integer offset from the start of the query to return
#'
#' @return JSON object as a list
#' @importFrom httr modify_url GET content add_headers
#' @importFrom future availableWorkers
p2_query_api <- function(url, api_key = keyring::key_get("P2_API"), offset = 0) {
  len <- off <- NULL

  api_headers <- add_headers("Api-Token" = api_key)

  first <- modify_url(url, query = list("limit" = 1)) %>%
    GET(api_headers) %>%
    content()
  if (is.null(first$meta)) {
    total <- map_int(first, length) %>% max()
    by <- total
  } else {
    total <- as.integer(first$meta$total)
    by <- 100
  }

  jobs <- data.table(off = seq(offset, total, by = by))
  jobs <- jobs[, len := c(off[-1], total) - off][len > 0]

  p <- progressor(sum(jobs$len) + 1)
  p(paste("Querying", url))

  furrr::future_map2(jobs$off, jobs$len, ~ {
    res <- GET(modify_url(url, query = list("offset" = .x, "limit" = .y)), api_headers) %>%
      content() %>%
      map(p2_json_to_datatable)
    p(amount = .y)
    res
  }) %>% p2_combine_jsons()
}

#' p2_combine_jsons
#'
#' Combine `JSON` objects returned by the Active Campaign API, concatenating each element with the same name.
#'
#' @param jsons list of *named* lists of data.tables, as returned by p2_json_to_datatable
#'
#' @return single `JSON` object as a list
#' @importFrom purrr map_int
#' @importFrom checkmate assert_true
#' @importFrom stats setNames
p2_combine_jsons <- function(jsons) {
  assert_true(all(map_int(jsons, ~ length(names(.))) > 0))

  # combine results
  names <- do.call(c, map(jsons, names)) %>% unique()
  map(setNames(names, names), ~ {
    name <- .
    rbindlist(map(jsons, name), fill = TRUE)
  })
}

#' p2_json_to_datatable
#'
#' Convert P2 JSON objects to a data table
#'
#' @param json list of P2 JSON objects
#' @importFrom purrr map discard keep
#' @importFrom data.table as.data.table
p2_json_to_datatable <- function(json) {
  if (!is.list(json)) {
    return(NULL)
  }

  keep(json, is.list) %>%
    do.call(what = rbind) %>%
    # convert to datatable
    as.data.table()
}

#' p2_db_open
#'
#' Open the local P2 email database
#'
#' @param db_path path of the SQLite database
#'
#' @return invisible
#'
p2_db_open <- function(db_path = tessilake::cache_path("p2.sqlite", "deep", "stream")) {

  if (is.null(tessistream$p2_db)) {
    if (!dir.exists(dirname(db_path))) {
      warning(paste("Creating path", dirname(db_path)))
      dir.create(dirname(db_path))
    }

    tessistream$p2_db <- DBI::dbConnect(RSQLite::SQLite(), db_path)
    # Set sqlite timeout to 5 seconds
    RSQLite::sqliteSetBusyHandler(tessistream$p2_db, 5000)
  }

  invisible()
}

#' @describeIn p2_db_open Close the local P2 email database
p2_db_close <- function() {
  if (!is.null(tessistream$p2_db)) {
    DBI::dbDisconnect(tessistream$p2_db)
    tessistream$p2_db <- NULL
  }
  invisible()
}

#' p2_db_update
#'
#' Write to the local P2 email database, either creating a new table or upserting into an existing one
#'
#' @param data data.frame of data to write to the database
#' @param table table name
#'
#' @return invisible
#' @importFrom checkmate assert_names assert_data_frame
#' @importFrom purrr walk
#' @importFrom dplyr distinct
p2_db_update <- function(data, table) {
  p2_db_open()
  if (is.null(data)) {
    return(invisible())
  }

  assert_data_frame(data)
  assert_names(colnames(data), must.include = "id")

  # unnest columns
  walk(colnames(data), ~ {
    data <<- p2_unnest(data, .)
  })

  data <- distinct(data)

  if (table %in% DBI::dbListTables(tessistream$p2_db)) {
    sqlite_upsert(tessistream$p2_db, table, data)
  } else {
    dplyr::copy_to(tessistream$p2_db, data, table, unique_indexes = list("id"), temporary = FALSE)
  }

  invisible()
}

#' p2_unnest
#'
#' Unnest a nested data.table ... this might be a useful function for other purposes but will need testing. For now it should at least
#' work with the nested structures that come from P2 JSONs
#'
#' @param data data.table
#' @param colname character, column to unnest
#'
#' @importFrom checkmate assert_data_table assert_choice
#' @importFrom rlang is_atomic list2
#' @importFrom purrr modify modify_if flatten map_lgl
#' @importFrom stats setNames
#' @importFrom data.table rbindlist
#'
#' @return unnested data.table, modified in place (unless the column needs to be unnested longer)
p2_unnest <- function(data, colname) {
  . <- NULL

  assert_data_table(data)
  assert_choice(colname, colnames(data))

  col <- data[, get(colname)]

  # if column is not a list then there's nothing to do
  if (is_atomic(col)) {
    return(data)
  }

  # rlang::inform(paste("Unnesting",colname))

  # replace nulls up to the second depth with NAs because nulls are only valid in lists
  col <- modify_if(col, ~ length(.) == 0, ~NA)
  col <- modify_if(col, ~ is.list(.), ~ modify_if(., ~ length(.) == 0, ~NA))

  # if any element is a list then it's a list column!
  if (any(map_lgl(col, is.list))) {
    if (is.null(names(col[[1]]))) {
      other_colnames <- setdiff(colnames(data), colname)
      data[, I := .I]
      data <- data[, list2(!!colname := flatten(col[I])), by = I][data,
        c(colname, other_colnames),
        on = "I", with = F
      ] %>% p2_unnest(colname)
    } else {
      col <- rbindlist(col, fill = T) %>% setNames(paste(colname, names(.), sep = "."))
      data <- cbind(data[, -colname, with = F], col)
    }
  } else {
    # have to plunk the whole column to get typing correct
    data[, (colname) := unlist(col)]
  }

  data
}

#' p2_update
#'
#' Incrementally update all of the p2 data in the local sqlite database
#'
#' @importFrom dplyr tbl summarize collect filter
#' @importFrom lubridate today dmonths
#' @importFrom dplyr select
p2_update <- function() {
  updated_timestamp <- id <- linkclicks <- NULL

  # not immutable or filterable, just reload the whole thing
  p2_load("campaigns")
  p2_load("messages")
  p2_load("links")
  p2_load("lists")
  p2_load("bounceLogs")
  p2_load("contactLists")
  p2_load("fieldValues", path = "api/3/fieldValues", query = list("filters[fieldid]" = 1))

  # has a date filter
  contacts_max_date <- if (DBI::dbExistsTable(tessistream$p2_db, "contacts")) {
    tbl(tessistream$p2_db, "contacts") %>%
      summarize(max(updated_timestamp, na.rm = TRUE)) %>%
      collect()
  } else {
    "1900-01-01"
  }
  p2_load("contacts", query = list("filters[updated_after]" = as.character(contacts_max_date)))

  # immutable, just load new ids
  for (table in c("logs", "linkData")) {
    max_id <- if (DBI::dbExistsTable(tessistream$p2_db, table)) {
      tbl(tessistream$p2_db, table) %>%
        summarize(max(as.integer(id), na.rm = TRUE)) %>%
        collect() %>%
        as.integer()
    } else {
      0
    }
    p2_load(table, offset = max_id)
  }

  # linkData is *mostly* immutable, so lets refresh the recent links...
  if (DBI::dbExistsTable(tessistream$p2_db, "links")) {
    recent_links <- tbl(tessistream$p2_db, "links") %>%
      filter(linkclicks > 0 & updated_timestamp > !!(today() - dmonths(1))) %>%
      select(id) %>%
      collect()

    p <- progressor(nrow(recent_links))
    map(recent_links$id, ~ {
      path <- paste0("api/3/links/", ., "/linkData")
      p(paste("Querying", path))
      p2_load("linkData", path = path)
    })
  }
}

#' p2_load
#'
#' Load p2 data from `api/3/{table}`, modified by arguments in `...` to the matching `table` in the local database
#'
#' @param table character, table to update
#' @param offset integer number of rows to offset from beginning of query
#' @param ... additional parameters to pass on to modify_url
#'
#' @importFrom rlang list2 `%||%` call2
p2_load <- function(table, offset = 0, ...) {
  . <- NULL

  # fresh load of everything
  args <- list2(...)
  args$path <- args$path %||% paste0("api/3/", table)
  args$url <- args$url %||% api_url

  p2_query_api(eval(call2("modify_url", !!!args)), offset = offset) %>%
    {
      p2_db_update(.[[table]], table)
    }
}

#' p2_email_map
#'
#' Constructs a one-to-many mapping between email addresses / P2 subscriber ids and Tessi customer numbers
#'
#' @return data.table of a mapping between email addresses and customer numbers
#' @export
#' @importFrom dplyr distinct select transmute
#' @importFrom data.table setDT
p2_email_map <- function() {

  primary_ind <- address <- customer_no <- . <- email <- id <- value <- contact <- i.customer_no <- group_customer_no <- i.group_customer_no <- NULL

  p2_db_open()

  # load data
  emails <- tessilake::read_tessi("emails",freshness = 0) %>%
    filter(primary_ind == "Y") %>%
    select(email = address, customer_no) %>%
    collect() %>%
    setDT() %>% .[,email:=tolower(trimws(email))]

  contacts <- tbl(tessistream$p2_db, "contacts") %>%
    transmute(
      id = as.integer(id),
      email = tolower(trimws(email))
    ) %>%
    collect() %>%
    setDT()

  customer_nos <- tbl(tessistream$p2_db, "fieldValues") %>%
    transmute(
      customer_no = as.integer(value),
      id = as.integer(contact)
    ) %>%
    collect() %>%
    setDT()

  # data between Tessi and p2 is *eventually* consistent but can get out of sync.
  # - if someone changes their email address in Tessi it might not get changed in P2
  # in that case, we want to rely on the customer number saved in P2 because that still
  # points to the correct customer.
  # - if someone changes their email address in P2 it might not get changed in Tessi
  # in that case we want to rely on the customer number saved in P2, because that still
  # points to the correct customer.
  # - email addresses are unique in P2, customer numbers are unique in Tessi. In the
  # case where multiple customers point to one email address, we want to expand to
  # all of the customer numbers so that we are tracking behavior across accounts...

  # link P2 customer_nos and contacts
  contacts[customer_nos, `:=`(
    customer_no = i.customer_no
  ), on = "id"]

  contacts <- contacts[!is.na(customer_no) & !is.na(email)]

  email_map <- rbind(contacts, emails[contacts$email,,on="email"], fill = T)

  # fill in subscriber id
  email_map <- email_map[,id:=max(id,na.rm=T),by="email"] %>%
    .[!is.na(customer_no) & !is.na(email)]

  # map customer_no -> group_customer_no
  email_map[tessilake::tessi_customer_no_map() %>%
              collect() %>%
              setDT(), group_customer_no := i.group_customer_no, on = c("customer_no" = "group_customer_no")]


  distinct(email_map)
}

#' p2_stream_build
#'
#' @return p2_stream as data.table
#'
#' @importFrom data.table setDT
#'
p2_stream_build <- function() {

  unixepoch <- tstamp <- subscriberid <- campaignid <- messageid <- link <- isread <- times <- ip <- ua <-
    uasrc <- referer <- email <- status <- updated_timestamp <- contact <- campaign <- event_subtype <- timestamp <- NULL

  # group_customer_no
  # timestamp : date of email event
  # event_type : Email
  # event_subtype : Open|Click|Unsubscribe|Hard Bounce|Forward
  # event_subtype2 : Onsale|Newsletter|Film|Fundraising
  # campaign_desc
  # url
  # domain

  p2_db_open()

  sends <- tbl(tessistream$p2_db, "logs") %>%
    transmute(
      timestamp = unixepoch(tstamp),
      subscriberid = as.integer(subscriberid),
      campaignid = as.integer(campaignid),
      messageid = as.integer(messageid)
    ) %>%
    collect() %>%
    setDT()

  events <- tbl(tessistream$p2_db, "linkData") %>%
    filter(messageid != "0") %>%
    transmute(
      timestamp = unixepoch(tstamp),
      subscriberid = as.integer(subscriberid),
      campaignid = as.integer(campaignid),
      messageid = as.integer(messageid),
      linkid = as.integer(link),
      isread = as.logical(isread),
      times = as.integer(times),
      ip, ua, uasrc, referer
    ) %>%
    collect() %>%
    setDT()

  bounces <- tbl(tessistream$p2_db, "bounceLogs") %>%
    filter(email!="") %>%
    transmute(
      timestamp = unixepoch(tstamp),
      subscriberid = as.integer(subscriberid)
    ) %>%
    collect() %>%
    setDT()

  unsubs <- tbl(tessistream$p2_db, "contactLists") %>%
    filter(status != "1") %>%
    transmute(
      timestamp = unixepoch(updated_timestamp),
      status = as.integer(status),
      subscriberid = as.integer(contact),
      campaignid = as.integer(campaign),
      messageid = as.integer(message),
      listid = as.integer(list)
    ) %>%
    collect() %>%
    setDT()

  p2_stream <- rbind(
    bounces[, event_subtype := "Soft Bounce"],
    unsubs[, event_subtype := ifelse(status == 2, "Unsubscribe", "Hard Bounce")],
    events[, event_subtype := ifelse(isread, "Open", "Click")],
    sends[, event_subtype := "Send"],
    fill = T)

  setkey(p2_stream, subscriberid, timestamp)
  p2_stream[, `:=`(timestamp = lubridate::as_datetime(timestamp),
                   event_type = "Email")]

  p2_stream <- merge(p2_stream, p2_email_map(), by.x="subscriberid", by.y="id", all.x=T, allow.cartesian = TRUE)

  p2_stream
}

#' p2_stream_enrich
#'
#' @param p2_stream stream data.table from p2_stream_build
#' @importFrom tessilake setleftjoin
#' @importFrom data.table setDT
#' @return stream data.table with added descriptive columns
p2_stream_enrich <- function(p2_stream) {
  name <- screenshot <- id <- send_amt <- total_amt <- opens <- uniqueopens <- linkclicks <- uniquelinkclicks <-
    subscriberclicks <- forwards <- uniqueforwards <- hardbounces <- softbounces <- unsubscribes <- . <- subject <-
    preheader_text <- link <- NULL

  campaigns <- tbl(tessistream$p2_db, "campaigns") %>% select(campaign_name=name,screenshot,
                                                              id,send_amt,total_amt,opens,uniqueopens,linkclicks,uniquelinkclicks,
                                                              subscriberclicks,forwards,uniqueforwards,hardbounces,softbounces,unsubscribes) %>%
    collect %>%
    setDT %>%
    .[,(3:ncol(.)):=lapply(.SD,as.integer),.SDcols=3:ncol(.)]
  messages <- tbl(tessistream$p2_db, "messages") %>% transmute(id = as.integer(id),message_name=name,subject,preheader_text) %>% collect %>% setDT
  links <- tbl(tessistream$p2_db, "links") %>% transmute(id = as.integer(id),link_name=name,link) %>% collect %>% setDT
  lists <- tbl(tessistream$p2_db, "lists") %>% transmute(id = as.integer(id),list_name=name) %>% collect %>% setDT

  p2_stream <- setleftjoin(p2_stream,campaigns,by=c("campaignid"="id"))
  p2_stream <- setleftjoin(p2_stream,messages,by=c("messageid"="id"))
  p2_stream <- setleftjoin(p2_stream,links,by=c("linkid"="id"))
  p2_stream <- setleftjoin(p2_stream,lists,by=c("listid"="id"))

  p2_stream
}


#' p2_stream
#'
#' Update and build the p2 parquet files
#'
#' @return stream as a data.table
#' @export
p2_stream <- function() {

  withr::defer(future::plan(future::sequential()))
  future::plan(future::multisession)

  p2_stream <- p2_stream_build()
  tessilake:::cache_write(p2_stream,"p2_stream","deep","stream",overwrite = T)
  p2_stream_enriched <- p2_stream_enrich(p2_stream)
  tessilake:::cache_write(p2_stream,"p2_stream_enriched","deep","stream",overwrite = T)

  p2_stream

}

if (FALSE) {

  emails <- read_tessi("emails") %>%
    dplyr::collect() %>%
    setDT()
  inactive_emails <- emails[primary_ind == "N"][!emails[primary_ind == "Y"], on = "address"]

  p2_emails <- purrr::map_chr(contacts, "email") %>% unique()
  p2_orphan <- p2_emails[!trimws(tolower(p2_emails)) %in% emails[primary_ind == "Y", trimws(tolower(address))]]

  # Add "Orphan account" tag and zero out membership data
  p2_import <- data.frame(
    Email = p2_orphan,
    Tags = "Orphan Account",
    MEMBER_LEVEL = NA,
    RECOGNITION_AMOUNT = NA,
    INITIATION_DATE = NA,
    EXPIRATION_DATE = NA,
    CONSTITUENCY_STRING_WITH_AFFILIATES = NA,
    BENEFIT_PROVIDER = NA,
    CURRENT_STATUS = NA,
    CUSTOMER_LAST_GIFT_DT = NA,
    CUSTOMER_LAST_TICKET_DT = NA
  )

  write.csv(p2_import, "p2_import.csv", row.names = F, na = "")

  save.image()
}
