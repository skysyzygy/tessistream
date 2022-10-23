x <- sample(1:26,1e6,replace=TRUE)
x[sample(1e6,9e5)] <- NA
x <- data.table::as.data.table(matrix(x,1e5,10))
x$group <- rep(seq(nrow(x)/10),each=10)

test_that("setnafill errors if x is not a data.table",{
  expect_error(setnafill(as.data.frame(x)),"Must be a data.table")
})
test_that("setnafill errors if type is not `const`, `locf` or `nocb`",{
  expect_error(setnafill(x,"something"),"should be.+const.+locf.+nocb")
})
test_that("setnafill errors if cols are not in x",{
  expect_error(setnafill(x,cols=c("something","V1")),"subset of .+V1.+V10")
  debugonce(setnafill)
  expect_error(setnafill(x,cols=c(1:11)),"sd")
})
test_that("setnafill errors if by is not in x",{})
test_that("setnafill warns if we are trying a const/group fill",{})
test_that("setnafill warns if we are trying a locf/nocb and we gave fill",{})

test_that("setnafill const matches data.table::setnafill",{})
test_that("setnafill locf matches data.table::setnafill",{})
test_that("setnafill nocb matches data.table::setnafill",{})

test_that("setnafill const works with characters and factors",{})
test_that("setnafill locf works with characters and factors",{})
test_that("setnafill nocb works with characters and factors",{})

test_that("setnafill locf works with groups",{})
test_that("setnafill nocb works with groups",{})
