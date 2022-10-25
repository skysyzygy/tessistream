x <- sample(1:26,1e3,replace=TRUE)
x[sample(1e3,9e2)] <- NA
x <- data.table::as.data.table(matrix(x,1e2,10))

test_that("setnafill errors if x is not a data.table",{
  x <- data.table::copy(x)
  expect_error(setnafill(as.data.frame(x)),"Must be a data.table")
})
test_that("setnafill errors if type is not `const`, `locf` or `nocb`",{
  x <- data.table::copy(x)
  expect_error(setnafill(x,"something"),"should be.+const.+locf.+nocb")
})
test_that("setnafill errors if cols are not in x",{
  x <- data.table::copy(x)
  expect_error(setnafill(x,cols=c("something","V1")),"subset of.+V1.+V10",)
  expect_error(setnafill(x,cols=1:11),"not <= 10")
})
test_that("setnafill errors if by is not in x",{
  x <- data.table::copy(x)
  expect_error(setnafill(x,type="locf",by=c("something","V1")),"subset of.+V1.+V10",)
})
test_that("setnafill warns if we are trying a const/group fill",{
  x <- data.table::copy(x)
  expect_warning(setnafill(x,type="const",by=c("V1")),"argument.+by.+ignored.+const")
})
test_that("setnafill warns if we are trying a locf/nocb and we gave fill",{
  x <- data.table::copy(x)
  expect_warning(setnafill(x,type="locf",fill=1),"argument.+fill.+ignored.+const")
  expect_warning(setnafill(x,type="nocb",fill=1),"argument.+fill.+ignored.+const")
})

test_that("setnafill const matches data.table::setnafill and doesn't copy",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  tracemem(x)
  tracemem(y)
  setnafill(x,type="const",fill=1)
  data.table::setnafill(y,type="const",fill=1)
  untracemem(x)
  untracemem(y)
  expect_equal(x,y)
})
test_that("setnafill locf matches data.table::setnafill and doesn't copy",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  tracemem(x)
  tracemem(y)
  setnafill(x,type="locf")
  data.table::setnafill(y,type="locf")
  untracemem(x)
  untracemem(y)
  expect_equal(x,y)
})
test_that("setnafill nocb matches data.table::setnafill and doesn't copy",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  tracemem(x)
  tracemem(y)
  setnafill(x,type="nocb")
  data.table::setnafill(y,type="nocb")
  untracemem(x)
  untracemem(y)
  expect_equal(x,y)
})

char_cols = 1:3
cols = 1:6
test_that("setnafill const works with characters and factors",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  x <- x[,(char_cols):=lapply(.SD,function(.) {letters[.]}), .SDcols=char_cols]
  xf <- x[,(char_cols):=lapply(.SD,factor), .SDcols=char_cols]
  setnafill(x,cols=char_cols,type="const",fill="z")
  setnafill(xf,cols=char_cols,type="const",fill="z")
  x <- x[,(char_cols):=lapply(.SD,function(.) {match(.,letters)}), .SDcols=char_cols]
  xf <- x[,(char_cols):=lapply(.SD,as.integer), .SDcols=char_cols]
  expect_equal(x,data.table::setnafill(y,cols=char_cols,type="const",fill=26))
  expect_equal(xf,data.table::setnafill(y,cols=char_cols,type="const",fill=26))
})
test_that("setnafill locf works with characters and factors",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  x <- x[,(char_cols):=lapply(.SD,function(.) {letters[.]}), .SDcols=char_cols]
  xf <- x[,(char_cols):=lapply(.SD,factor), .SDcols=char_cols]
  setnafill(x,cols=cols,type="locf")
  setnafill(xf,cols=cols,type="locf")
  x <- x[,(char_cols):=lapply(.SD,function(.) {match(.,letters)}), .SDcols=char_cols]
  xf <- x[,(char_cols):=lapply(.SD,as.integer), .SDcols=char_cols]
  expect_equal(x,data.table::setnafill(y,cols=cols,type="locf"))
  expect_equal(xf,data.table::setnafill(y,cols=cols,type="locf"))

})
test_that("setnafill nocb works with characters and factors",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  x <- x[,(char_cols):=lapply(.SD,function(.) {letters[.]}), .SDcols=char_cols]
  xf <- x[,(char_cols):=lapply(.SD,factor), .SDcols=char_cols]
  setnafill(x,cols=cols,type="nocb")
  setnafill(xf,cols=cols,type="nocb")
  x <- x[,(char_cols):=lapply(.SD,function(.) {match(.,letters)}), .SDcols=char_cols]
  xf <- x[,(char_cols):=lapply(.SD,as.integer), .SDcols=char_cols]
  expect_equal(x,data.table::setnafill(y,cols=cols,type="nocb"))
  expect_equal(xf,data.table::setnafill(y,cols=cols,type="nocb"))
})

x$group <- rep(seq(nrow(x)/10),each=10)

test_that("setnafill locf works with groups",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  expect_mapequal(setnafill(x,cols=cols,type="locf",by="group"),
               y[,(cols):=data.table::nafill(.SD,type="locf"),by="group",.SDcols=cols])
})
test_that("setnafill nocb works with groups",{
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  expect_mapequal(setnafill(x,cols=cols,type="nocb",by="group"),
                  y[,(cols):=data.table::nafill(.SD,type="nocb"),by="group",.SDcols=cols])
})
