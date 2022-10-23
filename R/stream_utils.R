#' setnafill
#'
#' Wrapper for data.table's setnafill for fast filling by group. Accepts non-numeric columns
#'
#' @param x data.table
#' @param type character, one of "`const`", "`locf`" or "`nocb`". Defaults to "`const`".
#' @param fill vector, value to be used to fill when `type=="const"`.
#' @param cols numeric or character vector specifying columns to be updated.
#' @param by character string of columns to group by
#'
#' @return data.table, filled in-place
#' @export
#'
#' @examples
#' x <- data.table(a=NA,b=seq(1,10,10))
#' x[1,a:=1]
#' x[10,a:="x]
#' setnafill(x,"const","test",by="b")
#' setnafill(x,"locf",by="b")
#' setnafill(x,"nocb",by="b")
#'
#' @importFrom checkmate assert_data_table assert_names assert_vector assert_character assert assert_integer
setnafill <- function(x, type = c("const","locf","nocb"), fill=NA, cols=seq_along(x), by=NA) {
  assert_data_table(x)

  type = match.arg(type)
  if (type != "const" && !missing(fill))
    warning("argument 'fill' ignored, only make sense for type='const'")

  if (type == "const" && !missing(by))
    warning("argument 'by' ignored, doesn't make sense for type='const'")

  if (!is.na(by))
    assert_names(by,subset.of = colnames(x))

  assert(
    assert_names(cols,subset.of = colnames(x)),
    assert_integer(cols, lower = 1, upper = ncol(x))
  )

  if(is.integer(cols))
    cols = colnames(x)[cols]

  if(type == "const") {
    lapply(cols,function(.) { x[is.na(get(.)),(.) := fill] })
  } else {

    x[,I:=.I]
    setkey(x,I)
    roll = if_else(type == "locf",Inf,-Inf)

    lapply(cols,function(col) {
      i <- is.na(x$col)
      x[i,(col) := x[!i][x[i], get(col), on = na.omit(c(by, "I")), roll = roll]]
    })

  }

  x

}


