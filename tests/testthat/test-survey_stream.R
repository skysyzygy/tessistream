withr::local_package("checkmate")
withr::local_package("mockery")

# survey_find_column ------------------------------------------------------

test_that("survey_find_column identifies the maximum column based on `.f`", {
  survey_stream <- data.table(x=rep(1,100),y=runif(100))
  
  expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).))
  expect_equal(c("y"=2),survey_find_column(survey_stream,\(.)-.))
})

test_that("survey_find_column warns if more than one column meets the criterion", {
  survey_stream <- data.table(x=rep(1,100),y=runif(100),z=rep(1.1,100))
  expect_equal(c("z"=3),survey_find_column(survey_stream,\(.).,criterion="max"))
  expect_warning(expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).,criterion = 1)))
  
  survey_stream <- data.table(x=rep(1,100),y=runif(100),z=rep(1,100))
  expect_warning(expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).,criterion = 1)))
  expect_warning(expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).,criterion = "max")))
})


# survey_monkey -----------------------------------------------------------

test_that("survey_monkey returns a data.table of survey data", {
  expect_warning(survey_data <- survey_monkey(here::here("tests/testthat/survey_data/Audience Survey Spring 2024.xlsx")),
    "More than one column found")
  
  expect_data_table(survey_data)
  expect_names(colnames(survey_data),permutation.of=c("email","timestamp","question","subquestion","answer"))
})

test_that("survey_monkey identifies emails and timestamp columns", {
  expect_warning(survey_data <- survey_monkey(here::here("tests/testthat/survey_data/Audience Survey Spring 2024.xlsx")),
                 "More than one column found")
  
  expect_class(survey_data$timestamp,"POSIXct")
  expect_true(all(survey_data$timestamp>'2024-01-01'))
  expect_true(all(survey_data$timestamp<'2025-01-01'))
  expect_true(all(grepl("@",survey_data$email)))
})


# survey_stream -----------------------------------------------------------


#' ## Features include:
#' * customer_hash: string hash of customer_no
#' * group_customer_hash: string hash of group_customer_no
#' * timestamp: submission timestamp
#' * survey: filename / title of the survey
#' * question: text of the question
#' * subquestion: text of the subquestion (or NA if none)
#' * answer: text of the response
#' * encoded_answer: embedding of the answer (e.g. integers for likert scale)

test_that("survey_stream loads data from `survey_dir`", {
  suppressWarnings(survey_data <- survey_monkey(here::here("tests/testthat/survey_data/Audience Survey Spring 2024.xlsx")))
  survey_reader <- mock(survey_data,cycle=T)
  stub(survey_stream,"dir",c("a","b","c"))
  stub(survey_stream,"stream_from_audit",data.table(address=paste0(seq(1000),"@bam.org"),timestamp=Sys.Date(),
                                                    customer_no=seq(1000),group_customer_no=seq(1000)+10000,primary_ind="Y"))
  stub(survey_stream,"read_tessi",data.table(customer_no=seq(10000)))
  
  survey_stream(reader = survey_reader)
  
  expect_length(mock_args(survey_reader), 3)
  expect_equal(mock_args(survey_reader)[[1]][[1]],"a")
})

test_that("survey_stream identifies customers by email address", {
  suppressWarnings(survey_data <- survey_monkey(here::here("tests/testthat/survey_data/Audience Survey Spring 2024.xlsx")))
  survey_reader <- mock(survey_data,cycle=T)
  
  stream_from_audit <- data.table(address=paste0(seq(1000),"@bam.org"),timestamp=Sys.Date(),
                                  customer_no=10000+seq(1000),group_customer_no=100000+seq(1000),primary_ind="Y")
  stub(survey_stream,"stream_from_audit",stream_from_audit)
  stub(survey_stream,"read_tessi",data.table(customer_no=seq(11000)))
  stub(survey_stream,"anonymize",function(.).)
  
  survey_data <- survey_data[question != "Customer number"]
  
  survey_stream <- survey_stream(reader = survey_reader)
  
  expect_equal(survey_stream[customer_hash %in% stream_from_audit$customer_no,customer_hash],
               survey_data[email %in% stream_from_audit$address,as.integer(gsub("@bam.org","",email))+10000])
})

test_that("survey_stream fills in customer number if it has been collected as a question", {
  suppressWarnings(survey_data <- survey_monkey(here::here("tests/testthat/survey_data/Audience Survey Spring 2024.xlsx")))
  survey_reader <- mock(survey_data,cycle=T)
  
  stream_from_audit <- data.table(address=paste0(seq(1000),"@bam.org"),timestamp=Sys.Date(),
                                  customer_no=10000+seq(1000),group_customer_no=100000+seq(1000),primary_ind="Y")
  stub(survey_stream,"stream_from_audit",stream_from_audit)
  stub(survey_stream,"read_tessi",data.table(customer_no=seq(11000)))
  stub(survey_stream,"anonymize",function(.).)
  
  expect_warning(survey_stream <- survey_stream(reader = survey_reader),"Found customer number question.+Customer number")
  
  expect_equal(survey_stream[!customer_hash %in% stream_from_audit$customer_no,customer_hash],
               survey_data[question == "Customer number"] %>% 
                 .[survey_data[!email %in% stream_from_audit$address & !is.na(answer) & question != "Customer number"],
                   as.integer(answer),
                   on="email"]
  )
})

test_that("survey_stream anonymizes customer number", {
  suppressWarnings(survey_data <- survey_monkey(here::here("tests/testthat/survey_data/Audience Survey Spring 2024.xlsx")))
  survey_reader <- mock(survey_data,cycle=T)
  
  stream_from_audit <- data.table(address=paste0(seq(1000),"@bam.org"),timestamp=Sys.Date(),
                                  customer_no=10000+seq(1000),group_customer_no=100000+seq(1000),primary_ind="Y")
  stub(survey_stream,"stream_from_audit",stream_from_audit)
  stub(survey_stream,"read_tessi",data.table(customer_no=seq(11000)))

  expect_warning(survey_stream <- survey_stream(reader = survey_reader),"Found customer number question.+Customer number")
  
  expect_true(all(nchar(survey_stream$customer_hash)==64))
  expect_true(all(nchar(survey_stream$group_customer_hash)==64))
  expect_failure(expect_contains(survey_stream$question,"Customer number"))
})

test_that("survey_stream returns a data.table", {
  suppressWarnings(survey_data <- survey_monkey(here::here("tests/testthat/survey_data/Audience Survey Spring 2024.xlsx")))
  survey_reader <- mock(survey_data,cycle=T)
  
  stream_from_audit <- data.table(address=paste0(seq(1000),"@bam.org"),timestamp=Sys.Date(),
                                  customer_no=10000+seq(1000),group_customer_no=100000+seq(1000),primary_ind="Y")
  stub(survey_stream,"stream_from_audit",stream_from_audit)
  stub(survey_stream,"read_tessi",data.table(customer_no=seq(11000)))
  
  expect_warning(survey_stream <- survey_stream(reader = survey_reader),"Found customer number question.+Customer number")
  
  expect_data_table(survey_stream)
  expect_names(colnames(survey_stream), permutation.of=c("customer_hash","group_customer_hash","timestamp","survey","question","subquestion"))
})
