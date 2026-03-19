# Module: Radiomics Profile Constructors
# Each returns a named list describing PyRadiomics settings.

#' IBSI-compliant CT 3D profile
#' @param bin_width Numeric; histogram bin width (default 25).
#' @return A radiomics profile spec.
#' @export
ds.radiomics.profile.ibsi_ct_3d <- function(bin_width = 25) {
  list(name = "ibsi_ct_3d_v1", bin_width = bin_width,
       force2D = FALSE, normalize = TRUE,
       resampled_spacing = c(1, 1, 1),
       feature_classes = c("firstorder", "glcm", "glrlm", "glszm", "gldm", "ngtdm"),
       image_types = c("Original", "LoG", "Wavelet"))
}

#' IBSI-compliant MR 3D profile
#' @export
ds.radiomics.profile.ibsi_mr_3d <- function(bin_width = 25) {
  list(name = "ibsi_mr_3d_v1", bin_width = bin_width,
       force2D = FALSE, normalize = TRUE,
       resampled_spacing = c(1, 1, 1),
       feature_classes = c("firstorder", "glcm", "glrlm", "glszm", "gldm"),
       image_types = c("Original", "LoG"))
}

#' Force-2D profile (for 2D slices)
#' @export
ds.radiomics.profile.force2d <- function(bin_width = 25) {
  list(name = "ibsi_force2d_v1", bin_width = bin_width,
       force2D = TRUE, normalize = TRUE,
       feature_classes = c("firstorder", "glcm", "glrlm"),
       image_types = c("Original"))
}

#' Voxel-based feature map (firstorder only)
#' @export
ds.radiomics.profile.voxel_firstorder <- function(bin_width = 25, kernel_radius = 2) {
  list(name = "voxel_map_firstorder_v1", bin_width = bin_width,
       voxel_based = TRUE, kernel_radius = kernel_radius,
       feature_classes = c("firstorder"),
       image_types = c("Original"))
}
