tessistream <- new.env()

# load progressor if it's available
progressor <- if(system.file(package="progressr") != "") {
  progressr::progressor
} else {
  function(...){function(...){}}
}
