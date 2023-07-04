tessistream <- new.env()

# load progressor if it's available
progressor <- if(system.file(package="progressr") != "") {
  progressr::progressor
} else {
  function(...){function(...){}}
}

#' progress_expr
#'
#' Wrapper for incrementing progress after the end of `expr` evaluation.
#'
#' @param expr expression to evaluate
#' @param .progress progression function to call after each run of expr
#'
#' @return result of `expr`
#' @export
progress_expr <- function(expr, .progress) {
  checkmate::assert_function(.progress)

  res <- eval(expr)
  .progress()
  return(res)
}
