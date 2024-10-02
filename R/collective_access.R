#' ca_stream
#' 
#' Create a dataset from a CollectiveAccess instance. 
#'
#' @param ca_table [character](1) base CollectiveAccess table for the query (e.g. `ca_entities`, `ca_occurrences`, etc.)
#' @param query [character](1) search query 
#' @param base_url [character](1) CollectiveAccess base API endpoint 
#'        \href{https://manual.collectiveaccess.org/providence/developer/web_service_api.html#global-parameters}{ending in `service.php` or `service.php/json`}
#' @param login [character](1) in the format of `username:password` for authenticating with CollectiveAccess
#' @param features [list] of features to include in the dataset. List names will be used as the names of output columns, list 
#' values identify the bundle (or a list of bundles that will concatenated) for the output column. Each bundle identifier can itself
#' be a list in order to specify additional parameters (e.g. `template`, `delimiter`, etc.)
#' 
#' ## Example: 
#' ```
#' list(
#'      "idno" = "idno",
#'      "name" = "preferred_labels",
#'      "season" = "ca_occurrences.hierarchy.preferred_labels",
#'      "date" = list("productionDate","screeningDate"),
#'      "artists" = list("ca_entities" = list(
#'        "template" = "<unit relativeTo='ca_entities' delimiter='|'>
#'                          ^preferred_labels (^relationship_typename)
#'                      </unit>")
#'      )
#' )
#' ```
#' @inheritParams tessilake::write_cache
#' @export
ca_stream <- function(table_name = ca_table, ca_table, 
                      query = "*", 
                      features = NULL,
                      base_url = config::get("tessistream.ca_base_url"), 
                      login = config::get("tessistream.ca_login")) {
  assert_list(features, names = "named")
  
  # extract all the bundles from the featureset
  bundles <- setNames(features,NULL) %>% unlist(F)
  unnamed_bundles <- bundles[names(bundles) == ""]
  named_bundles <- bundles[names(bundles) != ""]
  
  # append default parameters
  default_params <- list("convertCodesToDisplayText" = T, "returnAsArray" = T)
  bundles <- c(setNames(unnamed_bundles,unnamed_bundles), named_bundles) %>%
    purrr::map(\(.) if (is.list(.)) {
      c(.,default_params)
    } else {
      default_params
    })
  
  records <- ca_search(ca_table = ca_table, query = query, base_url = base_url,
                       login = login) %>%
    split(rep(seq_len(nrow(.)),
              each = 1000,
              length.out = nrow(.)))
  
  results <- list()
  
  for (chunk in records) {
    results <- rbind(results,
                     ca_search(ca_table = ca_table,
                               query = paste0('(',ca_table,'.',occurrence_id,':[',
                                              chunk[,min(id)],' TO ',
                                              chunk[,max(id)],']) AND ',
                                              query),
                               base_url = base_url, login = login, bundles = bundles))
    
  }
  
  # assemble output dataset
  results <- results[,lapply(features,\(.) 
                             purrr::pmap(mget(names(.) %||% as.character(.), envir = as.environment(results)),
                                         ca_c))]
  
  
  write_cache(results, table, "stream")
  
}

#' @describeIn ca_stream login to a CollectiveAccess instance
#' @return [character] authorization token
#' @importFrom httr GET
ca_login <- function(login, base_url) {
  username <- strsplit(login,":")[[1]][1]
  password <- strsplit(login,":")[[1]][2]

  res <- GET(file.path(base_url,"auth","login"),username = username,password = password) %>%
    content

  if(!is.null(res) & res$ok)
    return(res$authToken)

}

#' @describeIn ca_stream execute a search query 
#' @return [data.table] with list columns corresponding to the items in `bundles`.
#' @param bundles [list] of bundles, as described in \href{https://manual.collectiveaccess.org/providence/developer/api_getting_data.html#bundle-specifiers}{the CollectiveAccess API documentation}
#' @importFrom httr POST
#' @importFrom purrr map_depth
ca_search <- function(ca_table, query, base_url, bundles = NULL, login = NULL) {
  res <- POST(file.path(base_url,"find",ca_table),
             query = list(q = query, authToken = ca_login(base_url = base_url, login = login)),
             body = list(bundles = bundles),
             encode = "json") %>%
    content

  if(!is.null(res) & res$ok) {
    # preserve list columns by using rbind
    do.call(rbind, res$result) %>% as.data.table %>% 
    # unlist elements and convert back to data.table
      lapply(sapply,unlist) %>% setDT
      
  }
}


#' @describeIn ca_stream Combine several input lists/vectors into a single flat vector, with no NULL,NA,or "" elements
#' @param ... lists/vectors to combine
ca_c <- function(...) {
  # get rid of NULLs
  vec <- unlist(c(...))
  # get rid of NAs
  vec <- vec[!is.na(vec)]
  # get rid of blanks & "null"s
  vec[sapply(vec,trimws) != "" & !grepl("^null$",vec)]
}

query <- '(ca_occurrences.type_id:"installation" OR
          ca_occurrences.type_id:"bam_internal" OR
          ca_occurrences.type_id:"movie" OR
          ca_occurrences.type_id:"production" OR
          ca_occurrences.type_id:"artist_residency" OR
          ca_occurrences.type_id:"special_event")'

features <- list("id" = "occurrence_id",
                 "idno" = "idno",
                 "name" = "preferred_labels",
                 "series" = list("series","minor_bam_programming","screen_series"),
                 "season" = "ca_occurrences.hierarchy.preferred_labels",
                 "date" = list("productionDate","screeningDate"),
                 "description" = "productionDescription",
                 "genre" = list("genre","performance_elements"),
                 "venue" = "venue",
                 "artists" = list("ca_entities" = list(
                   "template" = "<unit relativeTo='ca_entities' delimiter='|'>^preferred_labels (^relationship_typename)</unit>")),
                 "origin_language" = list("country_origin_list","productionLanguage","translation"))


