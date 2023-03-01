

#' tessi_changed_emails
#'
#' Returns list of primary emails changed since `since` date.
#'
#' @param since date after which we look for changed emails
#'
#' @return data.table of changed emails with columns `from`, `to`, and `customer_no`
#' @importFrom tessilake read_tessi
tessi_changed_emails <- function(since = Sys.Date() - 7) {

    a <- read_tessi("audit")
}

p2_update_email <- function(from = NULL, to = NULL, customer_no = NULL) {

}
