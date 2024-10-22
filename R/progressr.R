
#' progressr_
#'
#' Helper function to create a [progressr::progression] (or a dummy function) if progressr doesn't exist
#'
#' @param n [integer] number of loops
#' @return function to be called on each of n loops
progressr_ <- function(n) {
  
  if(system.file(package = "progressr") != "") {
    progressr::progressor(n, envir = parent.frame())
  } else {
    function(...) {}
  }
}
