#' duplicates_stream
#'
#' Create a dataset of duplicates derived from matching names, email addresses, postal addresses, and phone numbers. The match probability
#' for each probable duplicate is determined using the [fastLink] package.
#'
#' ### Deduplication using [fastLink]
#' This system is based on the [fastLink] package, a "Fast Probabilistic Record Linkage" library developed by
#' [Ted Enamorado, Ben Fifield, and Kosuke Imai](https://github.com/kosukeimai/fastLink).
#'
#' [fastLink] is designed to find matches between two datasets across multiple variables. It was designed for linking together political/social science datasets.
#' For deduplication, `duplicates_stream` compares the data against itself. Links will either be trivial \eqn{(A=A)} or they will be duplicates.
#'
#' [fastLink] identifies matches in a way that is sensitive to the structure of the data it is given: it assigns to each match pattern a probability that it is a match.
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
#' @param ... additional parameters passed on to [read_tessi] and [read_cache]
#' @export
duplicates_stream <- function(...) {



}

#' @describeIn duplicates_stream Load data for [duplicates_stream]
#' @usage NULL
#' @importFrom tessilake read_tessi read_sql_table read_cache
#' @importFrom dplyr filter collect
#' @note depends on [address_stream]
#' @return data.table of data for deduplication
duplicates_data <- function(...) {
  cust_type_desc <- fname <- lname <- eaddress_type <- NULL

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
  addresses <- addresses[!is.na(street1) & !is.na(group_customer_no),
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
}
