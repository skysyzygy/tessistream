% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/address_clean.R
\name{address_clean}
\alias{address_clean}
\title{address_clean}
\usage{
address_clean(address_col, pattern = "^(web add|unknown|no add)|^$")
}
\arguments{
\item{address_col}{character vector of address data}

\item{pattern}{perl-compatible regular expression to use for identifying junk address fields}
}
\value{
data.table of addresses cleaned
}
\description{
Removes newlines, tabs, lowercases, trims whitespace, and removes junk info
}
