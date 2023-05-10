withr::local_package("mockery")
withr::local_package("lubridate")

test_email = "ssyzygy@bam.org"

# setup -------------------------------------------------------------------

test_contact_id <- p2_query_api(modify_url(api_url, path = "api/3/contacts", query = list(email = test_email))) %>%
  {as.integer(.$contacts$id)}

test_tag_name <- rlang::hash(Sys.time())
suppressMessages(p2_execute_api(modify_url(api_url, path = "api/3/tags"), object = list(tag = list(tag = test_tag_name,
                                                                                  type = "contact",
                                                                                  description = "tag for P2 integration test"))))

test_tag_id <- p2_query_api(modify_url(api_url, path = "api/3/tags", query = list("filters[search][eq]" = test_tag_name))) %>%
  {as.integer(.$tags$id)}

withr::defer(suppressMessages(p2_execute_api(modify_url(api_url, path = file.path("/api/3/tags", test_tag_id)),
                                       method = "DELETE")))

test_that("setup worked", {
  expect_type(test_contact_id, "integer")
  expect_type(test_tag_id, "integer")
  expect_length(test_contact_id, 1)
  expect_length(test_tag_id, 1)
})

# p2_update_email ---------------------------------------------------------

test_that("p2_update_email updates an email", {
  new_email <- paste0(test_tag_name,"@test.com")

  expect_message(expect_true(p2_update_email(test_contact_id, new_email, dry_run = FALSE)))

  test_contact <- p2_query_api(modify_url(api_url, path = "api/3/contacts", query = list(email = new_email)))
  expect_equal(nrow(test_contact$contacts),1)

  expect_message(expect_true(p2_update_email(test_contact_id, test_email, dry_run = FALSE)))
  expect_equal(as.integer(test_contact$contacts$id),test_contact_id)
})

# p2_add_tag --------------------------------------------------------------

test_that("p2_add_tag complains if a tag doesn't exist", {
  expect_warning(p2_add_tag(test_contact_id, "I'm not really a tag", dry_run = TRUE),"Tag .+ not found")
})

test_that("p2_add_tag adds a tag", {
  expect_message(expect_message(p2_add_tag(test_contact_id, test_tag_name, dry_run = TRUE)))
  expect_message(p2_add_tag(test_contact_id, test_tag_name, dry_run = FALSE))

  test_contact <- p2_query_api(modify_url(api_url, path = "api/3/contacts", query = list(email = test_email,
                                                                                         include = "contactTags")))

  expect_true(test_tag_id %in% as.integer(test_contact$contactTags$tag))
})
