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

  survey_stream <- lapply(files, reader) %>% rbindlist(idcol = "filename")
  survey_stream[,survey := file_path_sans_ext(basename(filename))]

  emails <- stream_from_audit("emails", c("address","primary_ind")) %>%
    .[primary_ind=="Y" & !is.na(address), .(address, customer_no, group_customer_no, timestamp)]

  survey_stream <- emails[survey_stream,on=c("address"="email","timestamp"),roll=Inf]

  # find customer id question
  customers <- read_tessi("customers","customer_no") %>% collect
  customer_no_question <- survey_stream %>% dcast(...~question,value.var="answer") %>% 
    survey_find_column(\(.) as.numeric(.) %in% as.integer(customers$customer_no) & !duplicated(.) | is.na(.), criterion = .9*nrow(.)) %>% 
    names

  if (length(customer_no_question) > 0) {
    rlang::warn("Found customer number question, anonymizing:",body=c("*"=customer_no_question))
    # fill in missing customer info
    survey_stream[survey_stream[question == customer_no_question], `:=`(customer_no=coalesce(customer_no,as.numeric(i.answer))), on = "address"]
  }

  # and anonymize
  survey_stream[,`:=`(group_customer_hash = anonymize(group_customer_no),
                      customer_hash = anonymize(customer_no),
                      address = NULL,
                      customer_no = NULL,
                      group_customer_no = NULL)]
  
  if (length(customer_no_question) > 0) {
    survey_stream[question != customer_no_question]
  } else {
    survey_stream
  }

}


#' survey_monkey
#'
#' Parser for Surveymonkey exports, handles:
#'
#' * identifying `email` and `timestamp` columns
#' * appending survey name
#' * labeling questions and subquestions
#' * converting timestamps to POSIXct
#'
#' @param file filename of the file to process
#'
#' @returns data.table stream of survey data. Columns are:
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
  email_column <- survey_find_column(survey_data, \(.) grepl(email_regex,.,perl=T))

  # find timestamp column
  timestamp_column <- survey_find_column(survey_data, \(.) between(as.numeric(.),40000,50000))

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


#' survey_find_column
#' 
#' Find a column by applying `.f` to each element in the column, the results of which are summed, and then the column matching `criterion` is chosen.
#'
#' @param survey_data data.frame of survey data
#' @param .f function to apply to each element of `survey_data`, should return a numeric score
#' @param criterion numeric or "max". If "max" then the first column with a maximum score is chosen, otherwise the first column with a value greater than 
#' `criterion` is chosen.
#'
#' @return named integer vector, name is the selected column name and value is its position in `survey_data`
survey_find_column <- function(survey_data, .f, criterion = "max") {
  
  assert_data_frame(survey_data)

  columns <- sapply(survey_data,\(.) sum(suppressWarnings(.f(.)),na.rm=T))
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


#' survey_cross
#'
#' @param survey_stream data.table of survey data
#' @param ... one or more regular expressions used to locate the questions to combine
#'
#' @return data.table of survey data, merged on `customer_hash` so that each question/answer is 
#' a separate column, labelled `question.1`, `answer.1`, `question.2`, `answer.2`, etc. based on the
#' position of the selecting regular expression in `...`
#' @export
survey_cross <- function(survey_stream, ...) {
  assert_data_table(survey_stream)
  
  questions <- list(...) %>% lapply(\(.) survey_stream[grepl(.,question)])
  
  purrr::reduce2(questions, seq_along(questions)[-1],
                 \(.x, .y, .i) merge(.x, .y, by = "customer_hash", all = T, suffixes = paste0(".",c(.i-1,.i))))
  
}


#' survey_append_tessi
#'
#' @param survey_stream data.table of survey data
#' @param tables character vector of tables to pass to [tessilake::read_tessi]
#' @param ... expressions defining features that will be appended to survey data
#'
#' @return data.table of survey data with features appended
#' @export
survey_append_tessi <- function(survey_data, tables, ...) {

  features <- rlang::enquos(...)

  tables <- lapply(tables,\(.) {
    table <- read_tessi(.) %>% collect %>% setDT %>%
      .[,`:=`(customer_hash = anonymize(customer_no),
              group_customer_hash = anonymize(group_customer_no))] %>%
      .[group_customer_hash %in% survey_data$group_customer_hash]
    
    # remove sensitive columns
    cols <- intersect(colnames(table),
                      c("address","email","customer_no","group_customer_no"))
    table[,(cols) := NULL]
    table })

  append <- purrr::reduce(tables,\(.x,.y) merge(.x,.y,all = T)) %>%
    .[,lapply(features,rlang::eval_tidy,data=.SD),by="group_customer_hash"]

  
  merge(survey_data, append, all.x = T)

}
