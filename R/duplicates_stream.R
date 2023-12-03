#' duplicates_stream
#'
#' Create a dataset of duplicates derived from matching names, email addresses, postal addresses, and phone numbers. The match probability
#' for each probable duplicate is determined using the \link[fastLink:fastLink-package]{fastLink} package.
#'
#' ### Deduplication using \link[fastLink:fastLink-package]{fastLink}
#' This system is based on the \link[fastLink:fastLink-package]{fastLink} package, a "Fast Probabilistic Record Linkage" library developed by
#' [Ted Enamorado, Ben Fifield, and Kosuke Imai](https://github.com/kosukeimai/fastLink).
#'
#' \link[fastLink:fastLink-package]{fastLink} is designed to find matches between two datasets across multiple variables. It was designed for linking together political/social science datasets.
#' For deduplication, `duplicates_stream` compares the data against itself. Links will either be trivial \eqn{(A=A)} or they will be duplicates.
#'
#' \link[fastLink:fastLink-package]{fastLink} identifies matches in a way that is sensitive to the structure of the data it is given: it assigns to each match pattern a probability that it is a match.
#' A match pattern is a unique way that the variables match or don’t match: for example, two records that match on first name, partially match on last name, one is missing
#' data for email, etc. The algorithm tabulates counts of every occurring combination of match / partial-match / non-match / missing data across every variable for every
#' combination of two records. It then uses a Bayesian expectation-maximization algorithm to determine the probability that each combination of matches / non-matches / etc.
#' identifies a proper match, using the overall expected match rate as a prior. Then, given a probability threshold, it outputs the identified matches.
#'
#' `duplicates_stream` is using this system as-is with some updates for Tessi-specific deduplication:
#' * it pulls multiple rows of data per customer from Tessitura in order to find hidden matches
#' * it cleans the data, removing missing/incomplete info and also suppresses certain data that produces many false positives (school addresses and @bam.org emails)
#' * it is trained based on successfully-merged records in the database
#' * it uses blocking on first name, and a second pass of simple email address matching in order to reduce the overall computation time and catch more duplicates
#' * it caches datasets to disk to reduce memory load
#' * it provides reporting that includes the match pattern and posterior probability for each pair
#'
#' @return data.table of duplicates
#' @importFrom tessilake write_cache
#' @aliases duplicates
#' @export
duplicates_stream <- function(...) {
  unit <- NULL
  duplicates_data <- duplicates_data(...)

  email_dupes <- duplicates_exact_match(duplicates_data, "email")

  # allow matching on empty unit
  duplicates_data[is.na(unit), unit:=""]

  exact_dupes <- duplicates_exact_match(duplicates_data, c("email","house_number","road","unit",
                                                           "city","state","fname","lname"))


  dupes <- duplicates_suppress_related(exact_dupes) %>%
    duplicates_append_data()

  write_cache(dupes, "duplicates_stream", "stream", overwrite = TRUE)

  dupes
}

#' @describeIn duplicates_stream
#' Chooses which of each duplicate pair to keep or delete based
#' on appended data and return the reason for the choice.
#' @param features sorted, named list of expressions to use for selecting which customer
#' to keep. Note: this is not ready to be exposed; at the moment the appended data is very limited.
#'
#' @return data.table
duplicates_append_data <- function(data, features = rlang::exprs(
  "Current membership" = !is.na(memb_level),
  "Last login date" = last_login_dt,
  "Last activity date" = last_activity_dt,
  "Last update date" = last_update_dt)) {

  . <- memb_level <- last_login_dt <- last_activity_dt <- last_update_dt <-
    cur_record_ind <- customer_no <- memb_amt <- inactive <- inactive_desc <-
    keep_customer_no <- i.customer_no <- NULL

  memberships <- read_tessi("memberships") %>%
    filter(cur_record_ind == 'Y') %>% collect %>% setDT %>%
    setkey(customer_no,memb_amt) %>%
    .[,.SD[.N],.SDcols="memb_level",by="customer_no"]

  logins <- read_tessi("logins") %>%
    filter(inactive == 'N') %>% collect %>% setDT %>%
    setkey(customer_no,last_login_dt) %>%
    .[, .(last_login_dt = max(last_login_dt)),by="customer_no"]

  customers <- read_tessi("customers") %>%
    filter(inactive_desc == "Active") %>%
    select(customer_no, last_activity_dt, last_update_dt) %>%
    collect %>% setDT

  append_data <- merge(memberships,logins,by="customer_no",all=T) %>%
    merge(customers,by="customer_no",all=T)

  data[,keep_customer_no := NA_integer_]

  purrr::imap(features, \(feature, name) {
    lhs <- merge(data, append_data, all.x=T,
                 by.x="customer_no", by.y="customer_no") %>%
      .[,eval(feature)]
    rhs <- merge(data, append_data, all.x=T,
                 by.x="i.customer_no", by.y="customer_no") %>%
      .[,eval(feature)]

    data[lhs>rhs & is.na(keep_customer_no),
         `:=`(keep_customer_no = customer_no,
              delete_customer_no = i.customer_no,
              keep_reason = name)]
    data[rhs>lhs & is.na(keep_customer_no),
         `:=`(keep_customer_no = i.customer_no,
              delete_customer_no = customer_no,
              keep_reason = name)]

  })

  data
}

#' @describeIn duplicates_stream Suppress duplicates that are related by household
#' or that have a relationships (in Tessi-speak: an association) with each other.
duplicates_suppress_related <- function(data) {
  . <- group_customer_no.x <- group_customer_no.y <- customer_no <- i.customer_no <-
    associated_customer_no <- NULL

  households <- tessi_customer_no_map()

  dupes_in_households <-
    merge(data,households,all.x=T,
          by.x="customer_no", by.y="customer_no") %>%
    merge(households,all.x=T,
          by.x="i.customer_no",by.y="customer_no") %>%
    .[group_customer_no.x==group_customer_no.y, .(customer_no, i.customer_no)]

  relationships <- read_sql_table("VT_ASSOCIATION","BI") %>% collect %>%
    select(customer_no = customer_no, i.customer_no = associated_customer_no) %>%
    setDT

 data[!rbind(relationships, dupes_in_households),
      on = c("customer_no", "i.customer_no")]
}

#' @describeIn duplicates_stream
#' Returns a minimal (in the sense of each duplicate pairing is limited to the two
#' closest customer numbers in a duplicate cluster, and each pair is listed only
#' onc) set of customer pairs matching exactly on `match_cols`.
#'
#' @param data data.table to deduplicate
#' @param match_cols columns to match on
#'
#' @return data.table of matching customers with columns `customer_no` and `i.customer_no`
duplicates_exact_match <- function(data, match_cols) {

  . <- customer_no <- i.customer_no <- NULL

  assert_data_table(data)
  assert_names(names(data), must.include = match_cols)

  # Suppress rows with missing data
  data <- data[data[, rowSums(setDT(lapply(.SD,is.na))) == 0, .SDcols = match_cols]]

  # Do the match
  data[, i.customer_no := customer_no-.1]
  data[data,
       on = c(match_cols,"i.customer_no"="customer_no"),
       roll = -Inf] %>%
    .[!is.na(customer_no),.SD[1],
      by = c("customer_no", "i.customer_no"),
      .SDcols=c(match_cols)]

}

#' @describeIn duplicates_stream Load data for [duplicates_stream]
#' @usage NULL
#' @importFrom tessilake read_tessi read_sql_table read_cache
#' @importFrom dplyr filter collect
#' @note depends on [address_stream]
#' @return data.table of data for deduplication
#' @inheritDotParams tessilake::read_tessi freshness incremental
duplicates_data <- function(...) {
  . <- cust_type_desc <- fname <- lname <- eaddress_type <- inactive_desc <-
    customer_no <- group_customer_no <- phone <- address <- NULL

  # load individual names
  customers <- read_tessi("customers", ...) %>%
    filter(cust_type_desc == "Individual" &
             inactive_desc == "Active" &
             !is.na(fname) & fname != "" &
             !is.na(lname) & lname != "") %>%
    select(fname, lname, customer_no, group_customer_no) %>%
    collect(as_data_frame = F)

  # load all addresses past and present
  addresses <- read_cache("address_stream_full", "stream") %>%
    select(c(address_cols, "group_customer_no", "customer_no")) %>%
    collect %>% setDT %>%
  # and append libpostal parsed data
    cbind(address_parse(.)[,-address_cols, with = F])

  # rename libpostal columns
  addresses <- addresses[!is.na(group_customer_no),
      c("group_customer_no", grep("libpostal", colnames(addresses), value = TRUE)), with = F] %>%
    setnames(colnames(.),
             gsub("libpostal\\.","",colnames(.)))

  # load all phone numbers past and present
  phones <- stream_from_audit("phones", cols = "phone", ...) %>%
    .[!is.na(phone) & !is.na(group_customer_no), .(group_customer_no, phone)]

  # load all email addresses past and present
  emails <- stream_from_audit("emails", cols = c("address", "eaddress_type"), ...) %>%
    # Not assistant emails (eaddress_type = 6,7)
    .[!is.na(address) & !eaddress_type %in% c(6,7) & !is.na(group_customer_no), .(group_customer_no, email = address)]

  data <- merge(customers, addresses, by = "group_customer_no", all.x = T, suffixes = c("",".address"), allow.cartesian = TRUE) %>%
    merge(phones, by = "group_customer_no", all.x = T, suffixes = c("",".phone"), allow.cartesian = TRUE) %>%
    merge(emails, by = "group_customer_no", all.x = T, suffixes = c("",".email"), allow.cartesian = TRUE) %>%
    collect %>% setDT

  character_cols <- colnames(data)[sapply(data,is.character)]
  data <- data[, (character_cols) := lapply(.SD, \(x) trimws(tolower(x))), .SDcols = character_cols]
  for(col in character_cols)
    data[get(col)=="",(col) := NA]

  data
}

