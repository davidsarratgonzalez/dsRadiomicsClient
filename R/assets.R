# Module: Radiomics Asset Discovery
# Convenience wrappers around dsImagingClient for radiomics-specific queries.

#' List radiomics jobs
#' @param conns DSI connections object.
#' @return A dsjobs_result.
#' @export
ds.radiomics.list <- function(conns) {
  dsJobsClient::ds.jobs.list(conns, label = "dsRadiomics")
}

#' List radiomics feature tables for a dataset
#' @param conns DSI connections object.
#' @param dataset_id Character; dataset identifier.
#' @return Named list of per-server data.frames.
#' @export
ds.radiomics.features <- function(conns, dataset_id) {
  dsImagingClient::ds.imaging.catalog(conns, dataset_id, kind = "feature_table")
}

#' List segmentation masks for a dataset
#' @param conns DSI connections object.
#' @param dataset_id Character; dataset identifier.
#' @return Named list of per-server data.frames.
#' @export
ds.radiomics.masks <- function(conns, dataset_id) {
  dsImagingClient::ds.imaging.catalog(conns, dataset_id, kind = "mask_root")
}

#' Get radiomics capabilities from server
#' @param conns DSI connections object.
#' @return Named list of per-server capabilities.
#' @export
ds.radiomics.capabilities <- function(conns) {
  .ds_safe_aggregate(conns, expr = call("radiomicsCapabilitiesDS"))
}
