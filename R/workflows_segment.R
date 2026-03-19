# Module: Segmentation Workflow

#' Segment images in a dataset
#'
#' @param conns DSI connections object.
#' @param dataset_id Character; dataset identifier.
#' @param image_asset Character; asset_id or alias for images.
#' @param segmenter A segmenter from ds.segmenter.*().
#' @param visibility Character; "global" or "private".
#' @param alias Character or NULL; alias for the published mask.
#' @return A dsjobs_submission or existing asset_id.
#' @export
ds.radiomics.segment <- function(conns, dataset_id, image_asset = "images",
                                   segmenter, visibility = "global",
                                   alias = NULL) {
  if (identical(segmenter$provider, "existing_mask_asset")) {
    message("Using existing mask asset: ", segmenter$mask_asset)
    return(list(deduplicated = TRUE, asset_id = segmenter$mask_asset))
  }

  # Derivation hash
  hash <- dsImaging::compute_derivation_hash(
    dataset_id = dataset_id,
    image_asset = image_asset,
    provider = segmenter$provider,
    task = segmenter$task %||% "default",
    model_name = segmenter$model_name %||% segmenter$bundle_name %||% "default"
  )

  # Check existing
  existing <- dsImagingClient::ds.imaging.check_exists(conns, dataset_id,
    derivation_hash = hash)
  first_srv <- names(existing)[1]
  if (!is.null(existing[[first_srv]]) && isTRUE(existing[[first_srv]]$exists)) {
    message("Segmentation already computed: ", existing[[first_srv]]$asset_id)
    return(list(deduplicated = TRUE, asset_id = existing[[first_srv]]$asset_id))
  }

  # Map provider to runner
  runner <- switch(segmenter$provider,
    totalsegmentator = "totalsegmentator_infer",
    nnunetv2_predict = "nnunetv2_predict",
    monai_bundle_infer = "monai_bundle_infer",
    stop("Unknown segmentation provider: ", segmenter$provider, call. = FALSE))

  config <- segmenter
  config$image_asset <- image_asset

  job <- dsJobsClient::ds_job(
    label = "dsRadiomics",
    tags = c("segmentation", dataset_id, segmenter$provider),
    visibility = visibility,
    steps = list(
      dsJobsClient::ds_step_resolve_dataset(dataset_id),
      dsJobsClient::ds_step_run_artifact(runner, config = config),
      dsJobsClient::ds_step_publish_asset(dataset_id, "masks",
        asset_type = "mask_root", publish_kind = "imaging_asset"),
      dsJobsClient::ds_step_safe_summary()
    )
  )

  dsJobsClient::ds.jobs.submit(conns, job)
}
