withr::local_package("mockery")
withr::local_package("lubridate")

test_email = "ssyzygy@bam.org"


# tag helper functions --------------------------------------------------------

create_tag <- function(tag_name) {
  p2_execute_api(modify_url(api_url, path = "api/3/tags"), object = list(tag = list(tag = tag_name,
                                                                                    type = "contact",
                                                                                    description = "tag for P2 integration test")))
}

delete_tag <- function(tag_name) {
  tag_id <- p2_query_api(modify_url(api_url, path = "api/3/tags", query = list("filters[search][eq]" = tag_name))) %>%
    {as.integer(.$tags$id)}
  p2_execute_api(modify_url(api_url, path = file.path("/api/3/tags", tag_id)),method = "DELETE")
}

# setup -------------------------------------------------------------------

test_contact <- p2_query_api(modify_url(api_url, path = "api/3/contacts", query = list(email = test_email,
                                                                                       include = "fieldValues")))
test_contact_id <- as.integer(test_contact$contacts$id)
test_contact_no <- as.integer(test_contact$fieldValues[field == 1,value])

test_tag_name <- rlang::hash(Sys.time())
suppressMessages(create_tag(test_tag_name))
withr::defer(suppressMessages(delete_tag(test_tag_name)))

test_that("setup worked", {
  expect_type(test_contact_id, "integer")
  expect_type(test_tag_name, "character")
  expect_length(test_contact_id, 1)
  expect_length(test_tag_name, 1)
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
  test_tag_id <- p2_query_api(modify_url(api_url, path = "api/3/tags", query = list("filters[search][eq]" = test_tag_name))) %>%
    {as.integer(.$tags$id)}

  expect_true(test_tag_id %in% as.integer(test_contact$contactTags$tag))
})

# p2_resolve_orphan -------------------------------------------------------

test_that("p2_resolve_orphan updates an email", {
  new_email <- paste0(test_tag_name,"@test.com")

  expect_message(
    expect_message(
      expect_message(p2_resolve_orphan(test_email, new_email, customer_no = test_contact_no),paste("Updating",test_email,"to",new_email))
    ),
  new_email)

  test_contact <- p2_query_api(modify_url(api_url, path = "api/3/contacts", query = list(email = new_email)))
  expect_equal(nrow(test_contact$contacts),1)

  expect_message(expect_true(p2_update_email(test_contact_id, test_email, dry_run = FALSE)))
  expect_equal(as.integer(test_contact$contacts$id),test_contact_id)
})

test_that("p2_resolve_orphan adds a tag", {
  orphan_tag <- p2_query_api(modify_url(api_url, path = "api/3/tags", query = list("filters[search][eq]" = "Orphan Account")))
  tag_id <- as.integer(orphan_tag$tags$id)

  expect_message(
    expect_message(
      expect_message(p2_resolve_orphan(test_email, test_email, customer_no = test_contact_no),paste("Updating",test_email,"to",test_email))
    ), "contactTag")

  test_contact <- p2_query_api(modify_url(api_url, path = "api/3/contacts", query = list(email = test_email, include = "contactTags")))
  expect_true(tag_id %in% as.integer(test_contact$contactTags$tag))
  contact_tag_id <- as.integer(test_contact$contactTags[tag == tag_id,id])

  expect_message(p2_execute_api(modify_url(api_url, path = file.path("api/3/contactTags", contact_tag_id)), method = "DELETE"))

  test_contact <- p2_query_api(modify_url(api_url, path = "api/3/contacts", query = list(email = test_email, include = "contactTags")))
  expect_false(tag_id %in% as.integer(test_contact$contactTags$tag))
})
