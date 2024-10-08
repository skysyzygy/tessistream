% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/stream_utils.R
\name{stream_from_audit}
\alias{stream_from_audit}
\title{stream_from_audit}
\usage{
stream_from_audit(table_name, cols = NULL, ...)
}
\arguments{
\item{table_name}{character table name as in \code{tessilake::tessi_list_tables} \code{short_name} or \code{long_name}}

\item{cols}{character vector of columns that will be used from the audit and base table.
The names of the vector are the column names as identified in the audit table, the values are the column names as identified in the base table.
\emph{Default: column names from the audit table.}}

\item{...}{extra arguments passed on to \code{tessilake::read_tessi}}
}
\description{
Helper function to load data from the audit table and base table identified by \code{table_name}.
Produces a stream of creation/change/current state of all fields in the audit table in order
to reconstruct the state of a given element at some time in the past.
}
