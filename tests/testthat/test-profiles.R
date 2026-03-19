test_that("profile constructors return valid specs", {
  p1 <- ds.radiomics.profile.ibsi_ct_3d()
  expect_equal(p1$name, "ibsi_ct_3d_v1")
  expect_false(p1$force2D)
  expect_equal(p1$bin_width, 25)
  expect_true(length(p1$feature_classes) >= 5)

  p2 <- ds.radiomics.profile.force2d(bin_width = 50)
  expect_true(p2$force2D)
  expect_equal(p2$bin_width, 50)

  p3 <- ds.radiomics.profile.voxel_firstorder()
  expect_true(p3$voxel_based)
})

test_that("custom bin_width is preserved", {
  p <- ds.radiomics.profile.ibsi_ct_3d(bin_width = 100)
  expect_equal(p$bin_width, 100)
})
