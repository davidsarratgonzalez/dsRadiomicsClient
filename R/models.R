# Module: Segmentation Provider Constructors

#' Use existing mask asset (no segmentation needed)
#' @param mask_asset Character; asset_id or alias of existing masks.
#' @return A segmenter spec.
#' @export
ds.segmenter.existing_mask <- function(mask_asset) {
  list(provider = "existing_mask_asset", mask_asset = mask_asset)
}

#' TotalSegmentator segmenter
#' @param task Character; segmentation task (default "total").
#' @param fast Logical; use fast mode (default FALSE).
#' @param roi_subset Character vector or NULL; specific ROIs.
#' @return A segmenter spec.
#' @export
ds.segmenter.totalsegmentator <- function(task = "total", fast = FALSE,
                                            roi_subset = NULL) {
  list(provider = "totalsegmentator", task = task, fast = fast,
       roi_subset = roi_subset)
}

#' nnU-Net v2 segmenter
#' @param model_name Character; registered model name.
#' @param fold Character; fold to use (default "all").
#' @return A segmenter spec.
#' @export
ds.segmenter.nnunet <- function(model_name, fold = "all") {
  list(provider = "nnunetv2_predict", model_name = model_name, fold = fold)
}

#' MONAI bundle segmenter
#' @param bundle_name Character; registered bundle name.
#' @return A segmenter spec.
#' @export
ds.segmenter.monai_bundle <- function(bundle_name) {
  list(provider = "monai_bundle_infer", bundle_name = bundle_name)
}
