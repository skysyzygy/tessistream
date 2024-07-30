#' survey_stream
#'
#' Simple dataset of all survey results.
#'
#' @param survey_dir directory of surveys to parse
#' @param reader function(filename) that reads survey data, the only current reader is [survey_monkey]
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
#' so that the user *knows* what they are doing.
#'
#' @importFrom tools file_path_sans_ext
survey_stream <- function(survey_dir = config::get("tessistream")$survey_dir, reader = survey_monkey) {
  files <- dir(survey_dir,full.names=T,recursive=T) %>% setNames(.,.)

  #read_tessi <- mockery::mock(data.frame(customer_no=seq(20000)))
  #stream_from_audit <- mockery::mock(readRDS(rprojroot::find_testthat_root_file("email_stream-emails.Rds")) %>% setDT)

  survey_stream <- lapply(files, reader) %>% rbindlist(idcol = "filename")
  survey_stream[,survey := file_path_sans_ext(basename(filename))]

  emails <- stream_from_audit("emails", c("address","primary_ind")) %>%
    .[primary_ind=="Y" & !is.na(address), .(address, customer_no, group_customer_no, timestamp)]

  survey_stream <- emails[survey_stream,on=c("address"="email","timestamp"),roll=Inf]

  # find customer id question
  customers <- read_tessi("customers","customer_no") %>% collect
  customer_no_question <- survey_stream[,sum(sum(suppressWarnings(as.numeric(answer)) %in% as.integer(customers$customer_no) &
                                                   !duplicated(answer))),by="question"][V1 == max(V1),question]

  if (length(customer_no_question) > 1) {
    hashed_question <- customer_no_question[1]
    rlang::warn("More than on customer_no answer found, using:",body=c("*"=hashed_question[1]))
  }

  # fill in missing customer info
  survey_stream[survey_stream[question == customer_no_question], `:=`(customer_no=coalesce(customer_no,as.numeric(i.answer))), on = "address"]

  # and anonymize
  survey_stream[,`:=`(group_customer_hash = anonymize(group_customer_no),
                      customer_hash = anonymize(customer_no),
                      address = NULL,
                      customer_no = NULL,
                      group_customer_no = NULL)]
  survey_stream[question != customer_no_question]



}

email_fix_eaddress_stubbed <- function(email_stream) {
  emails <- readRDS(rprojroot::find_testthat_root_file("email_stream-emails.Rds")) %>% setDT
  stream_from_audit <- mock(emails)
  stub(email_fix_eaddress, "stream_from_audit", stream_from_audit)

  email_fix_eaddress(email_stream)
}

survey_cross <- function(survey_stream, ...) {
  questions <- list(...) %>% lapply(\(.) survey_stream[grepl(.,question)])

  purrr::reduce(questions, \(.x, .y) merge(.x, .y, by = "customer_hash", all = T))

}



#' survey_monkey
#'
#' Parser for Surveymonkey exports, handles:
#'
#' * identifying `email` and `timestamp` columns
#' * anonymizing customer id
#' * appending survey name
#' * labeling questions and subquestions
#' * converting timestamps to POSIXct
#'
#' @param file filename of the file to process
#'
#' @returns data.table stream of survey data, partially anonymized. Columns are:
#' * email
#' * timestamp
#' * question
#' * subquestion
#' * answer
#' @importFrom dplyr between
#' @importFrom data.table melt
survey_monkey <- function(file) {
  survey_data <- openxlsx::read.xlsx(file,sep.names = " ") %>%
    lapply(\(.) gsub("xml:space.+>","",.)) %>% setDT

  colnames(survey_data) <- gsub("xml:space.+>","",colnames(survey_data))

  # find email column
  email_regex <- "^[\\w\\-\\.]+@([\\w\\-]+\\.)+[\\w\\-]{2,4}$"
  email_column <- survey_find_column(survey_data, \(.) sum(grepl(email_regex,.,perl=T)))

  # find timestamp column
  timestamp_column <- survey_find_column(survey_data, \(.) sum(between(as.numeric(.),40000,50000), na.rm=T))

  # questions / sub-questions
  questions <- data.table(question = colnames(survey_data), # question is column name
                          subquestion = as.character(survey_data[1,]), # subquestion is first row
                          id = factor(seq_along(survey_data))) # id for joining
  # fill down missing questions
  questions[grepl("^X\\d+$",question),question:=NA]
  setnafill(questions, type = "locf", cols = "question")

  # relabel columns with ids
  colnames(survey_data) <- as.character(seq_along(survey_data))

  # melt data
  survey_stream <- survey_data[-1,] %>%
    melt(id.vars = c(email_column, timestamp_column),
         variable.name = "id", value.name = "answer") %>% left_join(questions, by="id")
  setnames(survey_stream, as.character(c(email_column, timestamp_column)),c("email","timestamp"))

  survey_stream <- survey_stream[!is.na(answer)]
  survey_stream[,`:=`(id = NULL,
                      timestamp = as.POSIXct(as.numeric(timestamp)*86400, origin = "1899-12-30 00:00:00"))]

  survey_stream
}


survey_find_column <- function(survey_data, .f, criterion = "max") {

  columns <- sapply(survey_data,\(.) suppressWarnings(.f(.)))
  column <- if (criterion == "max") {
    which(columns == max(columns))
  } else if (is.numeric(criterion)) {
    which(columns > criterion)
  } else {
    rlang::abort("Criterion must be numeric or `max`")
  }

  if (length(column) > 1) {
    column <- column[1]
    rlang::warn("More than one column found, using:",body=setNames(names(column),"*"))
  }

  return(column)
}

#' anonymize
#'
#' @param customer_no vector of customer numbers
#'
#' @return character vector of hashed numbers
#' @importFrom openssl sha256
#' @noRd
anonymize <- function(customer_no) {
  customer_no <- as.character(customer_no)
  salt = sha256(customer_no,key=sha256("this data is anonymized for privacy reasons"))
  customer_no = paste(salt,customer_no,sep="-")
  sha256(customer_no,key=sha256("please use it with care"))
}

survey_append_tessi <- function(survey_data, tables, ...) {

  features <- rlang::enquos(...)

  tables <- lapply(tables,\(.) read_tessi(.) %>% collect %>% setDT %>%
                     .[,`:=`(customer_hash = anonymize(customer_no),
                             group_customer_hash = anonymize(group_customer_no),
                             address = NULL,
                             customer_no = NULL,
                             group_customer_no = NULL)] %>%
                     .[group_customer_hash %in% survey_data$group_customer_hash])


  append <- purrr::reduce(tables,\(.x,.y) merge(.x,.y,all = T)) %>%
    .[,lapply(features,rlang::eval_tidy,data=.SD),by="group_customer_hash"]

  merge(survey_data, append, all.x = T)

}
