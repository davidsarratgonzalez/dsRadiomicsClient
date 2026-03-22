# Module: Radiomics Extraction Workflow
# Uses existing masks. Highest-value, lowest-risk path.

#' Extract radiomics features from a dataset
#'
#' Checks for existing identical derivation first (deduplication).
#' If not found, submits a dsJobs job.
#'
#' @param conns DSI connections object.
#' @param dataset_id Character; the dataset identifier.
#' @param image_asset Character; asset_id or alias for images (default "images").
#' @param mask_asset Character; asset_id or alias for masks.
#' @param profile A radiomics profile from ds.radiomics.profile.*().
#' @param visibility Character; job visibility label (default "global").
#' @param alias Character or NULL; alias for the published feature table.
#' @return A dsjobs_submission or existing asset_id if deduplicated.
#' @export
ds.radiomics.extract <- function(conns, dataset_id, image_asset = "images",
                                  mask_asset, profile = ds.radiomics.profile.ibsi_ct_3d(),
                                  visibility = "global", alias = NULL) {
  # Compute derivation hash
  hash <- dsImaging::compute_derivation_hash(
    dataset_id = dataset_id,
    image_asset = image_asset,
    mask_asset = mask_asset,
    profile_name = profile$name,
    bin_width = profile$bin_width,
    force2D = profile$force2D %||% FALSE,
    feature_classes = profile$feature_classes
  )

  # Check if already computed
  existing <- dsImagingClient::ds.imaging.check_exists(conns, dataset_id,
    derivation_hash = hash)
  first_srv <- names(existing)[1]
  if (!is.null(existing[[first_srv]]) && isTRUE(existing[[first_srv]]$exists)) {
    message("Radiomics already computed: ", existing[[first_srv]]$asset_id)
    return(list(deduplicated = TRUE, asset_id = existing[[first_srv]]$asset_id))
  }

  # Build job
  config <- c(profile, list(
    mask_asset = mask_asset, image_asset = image_asset,
    settings_file = profile$name
  ))

  job <- dsJobsClient::ds_job(
    label = "dsRadiomics",
    tags = c("extraction", dataset_id, profile$name),
    visibility = visibility,
    steps = list(
      dsJobsClient::ds_step_resolve_dataset(dataset_id),
      dsJobsClient::ds_step_run_artifact("pyradiomics_extract", config = config),
      dsJobsClient::ds_step_publish_asset(dataset_id, "radiomics",
        asset_type = "feature_table", publish_kind = "radiomics_asset"),
      dsJobsClient::ds_step_safe_summary()
    )
  )

  dsJobsClient::ds.jobs.submit(conns, job)
}
