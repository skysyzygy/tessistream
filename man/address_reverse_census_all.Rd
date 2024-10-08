% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/address_geocode.R
\name{address_reverse_census_all}
\alias{address_reverse_census_all}
\title{address_reverse_census_all}
\usage{
address_reverse_census_all(address_stream)
}
\arguments{
\item{address_stream}{data.table of addresses, must include \code{lat} and \code{lon}}
}
\value{
data.table of census geographies, one row per lat/lon pair in \code{address_stream}
}
\description{
Gets census geography (tract/block/county/state) information for US addresses using \link[censusxy:cxy_geography]{censusxy::cxy_geography}.
Calls \link{cxy_geography} once per row.
}
