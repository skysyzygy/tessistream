#' survey_stream
#'
#' Simple dataset of all survey results.
#'
#' @description
#' * anonymized using a hash so that joins can still be done post-hoc
#' * timestamped so that trends can be visualized
#'
#' ## Features include:
#' * customer_hash: string hash of customer_no
#' * group_customer_hash: string hash of group_customer_no
#' * timestamp: submission timestamp
#' * survey: filename / title of the survey
#' * question: text of the question
#' * subquestion: text of the subquestion (or NA if none)
#' * answer: text of the response
#' * encoded_answer: embedding of the answer (e.g. integers for likert scale)
#'
#' @note
#' There's no way to irreversibly anonymize this data and still allow post-hoc joins.
#' The secret in this case (the customer number) is stored openly in the database, the hashing
#' algorithm is explained here, and the number of possible customer numbers is small, so brute
#' forcing the mapping is trivial.
#'
#' The goal is just to make it more difficult to extract customer information from this table
#' so that the end user *knows* what they are doing.
#'
survey_stream <- function(survey_dir = config::get("tessistream")$survey_dir) {
  files <- dir(survey_dir,full.names=T,recursive=T)


}

#' anonymize
#'
#' @param customer_no vector of customer numbers
#'
#' @return character vector of hashed numbers
#' @importFrom openssl sha256
#' @noRd
anonymize <- function(customer_no) {
  salt = sha256(customer_no,key=sha256("this data is anonymized for privacy reasons"))
  customer_no = paste(salt,customer_no,sep="-")
  sha256(customer_no,key=sha256("please use it with care"))
}
