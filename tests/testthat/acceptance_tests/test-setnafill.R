nrow <- 1e6
ncol <- 1e1
ngroup <- nrow/10
x <- as.character(runif(nrow*ncol))
#x <- sample(letters,nrow*ncol,replace=T)
x[sample(length(x),.5*length(x))] <- NA
x <- data.table::as.data.table(matrix(x,nrow,ncol))


test_that("setnafill const uses the fastest option for character",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  z <- data.table::copy(x)
  time <- bench::mark(
    tessistream=setnafill(x,type="const",fill="a"),
    factor_character=setnafill_factor_character(y,type="const",fill="a"),
    const_simple=setnafill_const_simple(z,type="const",fill="a"),
    filter_gc = FALSE
  )
  expect_lte(time[1,"median"][[1]],min(time$median)*1.1)
})

test_that("setnafill locf uses the fastest option for character",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  z <- data.table::copy(x)
  time <- bench::mark(
    tessistream=setnafill(x,type="locf"),
    factor_character=setnafill_factor_character(y,type="locf"),
    group=setnafill_group(z,type="locf"),
    filter_gc = FALSE
    )
  expect_lte(time[1,"median"][[1]],min(time$median)*1.1)
})

test_that("setnafill nocb is faster than data.table::setnafill for character",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  z <- data.table::copy(x)
  time <- bench::mark(
    tessistream=setnafill(x,type="nocb"),
    factor_character=setnafill_factor_character(y,type="nocb"),
    group=setnafill_group(z,type="nocb"),
    filter_gc=FALSE
  )
  expect_lte(time[1,"median"][[1]],min(time$median)*1.1)
})

cols = colnames(x)[1:ncol]
x[,(cols):=lapply(.SD,function(.){match(.,letters)}),.SDcols=cols]
x$group <- rep(seq(ngroup),each=nrow/ngroup)

test_that("setnafill locf is faster than data.table::setnafill for groups",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  time <- bench::mark(
    tessistream=setnafill(x,type="locf",by="group"),
    data.table={
      y[,(cols):=data.table::nafill(.SD,type="locf"),.SDcols=cols,by="group"];
      y
    },
    filter_gc=FALSE)
  expect_lte(time[1,"median"],time[2,"median"])

})
test_that("setnafill nocb works with groups",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  time <- bench::mark(
    tessistream=setnafill(x,type="nocb",by="group"),
    data.table={
      y[,(cols):=data.table::nafill(.SD,type="nocb"),.SDcols=cols,by="group"];
      y
    },
    filter_gc=FALSE)
  expect_lte(time[1,"median"],time[2,"median"])
})
