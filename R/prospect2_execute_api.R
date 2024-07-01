#' @describeIn p2_verb_thing delete P2 tags
p2_delete_tags <- function(tags, dry_run = FALSE) {
  p2_verb_thing(tags,verb="DELETE",thing="tag",dry_run=dry_run)
}

#' @describeIn p2_verb_thing rename P2 tags
#' @param tags character vector of tags to update/delete
#' @param new_tags character vector of new names for `tags`
p2_rename_tags <- function(tags, new_tags, dry_run = FALSE) {
  new_tags <- map(new_tags,~list(tag=list(tag=.)))
  p2_verb_thing(tags,new_tags,verb="PUT",thing="tag",
                dry_run=dry_run)
}

#' @describeIn p2_verb_thing delete P2 list segments
#' @param segments character vector of segments to delete
p2_delete_segments <- function(segments, dry_run = FALSE) {
  p2_verb_thing(segments,verb="DELETE",
                thing="segment",dry_run=dry_run,
                .name = "name")
}


#' p2_verb_thing
#'
#' @param existing_things character vector of existing entity names
#' @param new_things list of objects to send to the API endpoint
#' @param verb HTTP verb to use for the API
#' @param .name character of name field that `existing_things` refers to
#' @param thing character of entity to update (at "api/3/{thing}")
#' @inheritParams p2_execute_api
#' @importFrom rlang list2
#' @importFrom checkmate assert_list
p2_verb_thing <- function(existing_things, new_things = NULL,
                          verb, thing,
                          dry_run = FALSE,
                          .name = thing) {
  assert_character(existing_things)
  if(is.null(new_things)) {
    new_things <- rep(list(NULL), length(existing_things))
  }
  assert_list(new_things, len = length(existing_things))

  assert_character(verb, len = 1)
  assert_character(thing, len = 1)

  things <- paste0(thing,"s")

  lookup <- p2_query_api(modify_url(api_url,
                                    path = paste0("api/3/",things)))[[things]]

  lookup[,name := unlist(get(.name))]

  jobs <- setDT(lookup)[existing_things,.(id, name,
                                          object = new_things), on="name"]

  if(any(sapply(jobs$id,is.null))) {
    rlang::warn(paste0("Cannot find listed ",things,", skipping:"),
                body = jobs[sapply(id,is.null), setNames(name, rep("*",.N))])

  }

  jobs <- jobs[!sapply(jobs$id,is.null)]

  purrr::map2_lgl(jobs$id, jobs$object, \(id,object) {
    p2_execute_api(url = modify_url(api_url,
                              path = paste0("api/3/",things,"/",id)),
                   method = verb,
                   object = object,
                   dry_run = dry_run)

  })
}


#' p2_execute_api
#'
#' Sends a JSON `object` to a P2 API endpoint `url` using an `api_key` and returns T/F based on `success_codes`
#'
#' @param url character, endpoint url for the API
#' @param object list to be converted into JSON using `jsonlite::toJSON`
#' @param success_codes, integer vector of success codes returned by the API
#' @param method, character name of the `httr` function to call, usually `POST` or `PUT`
#' @param api_key Active Campaign API key, defaults to `keyring::key_get("P2_API")`
#' @param dry_run boolean, nothing will be changed in P2 if set to `TRUE`
#'
#' @return `TRUE` if success, `FALSE` if not
#'
#' @importFrom checkmate assert_integerish
#' @importFrom rlang sym expr warn
p2_execute_api <- function(url, object = list(), success_codes = c(200,201,202), method = "POST",
                           api_key = keyring::key_get("P2_API"), dry_run = FALSE) {
  assert_character(url,len=1)
  assert_character(method,len=1)
  assert_character(api_key,len=1)

  api_headers <- add_headers("Api-Token" = keyring::key_get("P2_API"))
  inform(c(
    "v" = paste("Executing",method,":",url),
    "*" = jsonlite::toJSON(object, auto_unbox = T)
  ))

  if (dry_run) {
    inform(c("*" = "(dry run)"))
    return(TRUE)
  }

  fun <- eval(parse(text = paste0("httr::",method)))
  response <- fun(url = url, api_headers, body = object, encode = "json", httr::timeout(300))

  if (!response$status_code %in% success_codes) {
    warn(c("!" = paste(method, "to", url, "failed! Status code", response$status_code)), response = response)
    return(FALSE)
  }

  TRUE
}
