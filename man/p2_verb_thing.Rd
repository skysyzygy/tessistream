% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/prospect2_execute_api.R
\name{p2_delete_tags}
\alias{p2_delete_tags}
\alias{p2_rename_tags}
\alias{p2_delete_segments}
\alias{p2_verb_thing}
\title{p2_verb_thing}
\usage{
p2_delete_tags(tags, dry_run = FALSE)

p2_rename_tags(tags, new_tags, dry_run = FALSE)

p2_delete_segments(segments, dry_run = FALSE)

p2_verb_thing(
  existing_things,
  new_things = NULL,
  verb,
  thing,
  dry_run = FALSE,
  .name = thing
)
}
\arguments{
\item{tags}{character vector of tags to update/delete}

\item{dry_run}{boolean, nothing will be changed in P2 if set to \code{TRUE}}

\item{new_tags}{character vector of new names for \code{tags}}

\item{segments}{character vector of segments to delete}

\item{existing_things}{character vector of existing entity names}

\item{new_things}{list of objects to send to the API endpoint}

\item{verb}{HTTP verb to use for the API}

\item{thing}{character of entity to update (at "api/3/\if{html}{\out{<thing>}}")}

\item{.name}{character of name field that \code{existing_things} refers to}
}
\description{
p2_verb_thing
}
\section{Functions}{
\itemize{
\item \code{p2_delete_tags()}: delete P2 tags

\item \code{p2_rename_tags()}: rename P2 tags

\item \code{p2_delete_segments()}: delete P2 list segments

}}
