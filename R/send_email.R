#' send_email
#'
#' @param subject email subject
#' @param body email body
#' @param emails email addresses (first will be sender)
#' @param smtp smtp configuration
#' @param ... additional parameters sent on to `mailR::send.mail`
#' @importFrom checkmate assert_character test_character test_list
send_email <- function(subject, body,
                       emails = config::get("tessiflow.email"),
                       smtp = config::get("tessiflow.smtp"),...
                       ) {
  assert_character(subject, len = 1)
  assert_character(body, len = 1)

  if (!test_character(emails, min.len = 1)) {
    stop("Set tessiflow.email to the sender (first email) and list of recipients for error messages")
  }
  if (!test_list(smtp)) {
    stop("Set tessiflow.smtp to the smtp server used to send error messages")
  }

  send.mail(
    from = emails[[1]],
    to = emails,
    subject = paste0(subject, " (", Sys.info()["nodename"], ")"),
    body = body,
    smtp = smtp,
    encoding = "utf-8",
    ...,
    send = TRUE
  )
}

