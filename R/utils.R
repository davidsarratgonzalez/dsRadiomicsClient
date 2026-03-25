# Internal utilities (not exported)

`%||%` <- function(x, y) if (is.null(x)) y else x

#' Encode R objects as B64 JSON for DataSHIELD transport
#'
#' Always encodes (unlike dsFlowerClient's version which skips scalars).
#' dsRadiomics server-side expects B64-encoded arguments.
#'
#' @keywords internal
.ds_encode <- function(x) {
  json <- as.character(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"))
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  b64 <- gsub("\\+", "-", b64)
  b64 <- gsub("/", "_", b64)
  b64 <- gsub("=+$", "", b64)
  paste0("B64:", b64)
}

#' Resilient datashield.aggregate that tolerates per-server failures
#'
#' Accepts either a pre-built call expression or a method name + arguments.
#'
#' @param conns DSI connections object.
#' @param expr A call expression, OR a character method name (legacy).
#' @param ... Additional arguments if expr is a method name string.
#' @keywords internal
.ds_safe_aggregate <- function(conns, expr, ...) {
  # Support both styles:
  # .ds_safe_aggregate(conns, expr = call("methodDS", arg1, arg2))
  # .ds_safe_aggregate(conns, "methodDS", arg1, arg2)
  if (is.character(expr)) {
    args <- list(...)
    call_args <- c(list(as.name(expr)), args)
    expr <- as.call(call_args)
  }

  server_names <- if (inherits(conns, "DSConnection")) "default" else names(conns)
  results <- list()
  errors <- list()
  for (srv in server_names) {
    tryCatch({
      res <- DSI::datashield.aggregate(
        if (srv == "default") conns else conns[srv],
        expr = expr)
      results[[srv]] <- if (srv == "default") res else res[[srv]]
    }, error = function(e) {
      errors[[srv]] <<- e$message
    })
  }
  if (length(errors) > 0) attr(results, "ds_errors") <- errors
  results
}
