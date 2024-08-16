#' make_resilient
#'
#' Simple wrapper to retry an expression if it fails
#'
#' @param expr expression to try to run
#' @param num_tries number of times to attempt to run `expr`
#' @param sleep_secs numbe of seconds to sleep between tries
#' @param default value to return if `expr` fails `num_tries` times
#'
#' @return the result of `expr` or `default` depending on whether `expr` ever ran successfully
#' @export
make_resilient <- function(expr, num_tries = 10, sleep_secs = 10, default = NULL) {

  expr <- rlang::enexpr(expr)

  n <- 0
  while(n < num_tries) {
    result <- tryCatch(eval(expr, envir = parent.frame()),
                       error = \(e) e)
    if(!rlang::is_condition(result))
      break
    n <- n + 1
    Sys.sleep(sleep_secs)
  }

  if(rlang::is_condition(result)) {
    rlang::inform(rlang::cnd_message(result))
    return(default)
  }


  result

}
