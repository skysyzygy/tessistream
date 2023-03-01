

#' tessi_changed_emails
#'
#' Returns list of primary emails changed since `since` date.
#'
#' @param since date after which we look for changed emails
#'
#' @return data.table of changed emails with columns `old_value`, `new_value`, and `customer_no`
#' @importFrom tessilake read_tessi
tessi_changed_emails <- function(since = Sys.Date() - 7) {

    a <- stream_load_audit("T_EADDRESS") %>%
      .[(is.na(column_updated) | column_updated %in% c("primary_ind","address")) & !is.na(customer_no)]

    setnafill(a,"locf",cols=c("new_value","old_value"),by = "key_no")
    a[]

}

p2_update_email <- function(from = NULL, to = NULL, customer_no = NULL) {

}
