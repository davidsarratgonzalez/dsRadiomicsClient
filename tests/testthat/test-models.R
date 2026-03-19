test_that("segmenter constructors return valid specs", {
  s1 <- ds.segmenter.existing_mask("my_masks")
  expect_equal(s1$provider, "existing_mask_asset")
  expect_equal(s1$mask_asset, "my_masks")

  s2 <- ds.segmenter.totalsegmentator(task = "lung", fast = TRUE)
  expect_equal(s2$provider, "totalsegmentator")
  expect_equal(s2$task, "lung")
  expect_true(s2$fast)

  s3 <- ds.segmenter.nnunet("my_model")
  expect_equal(s3$provider, "nnunetv2_predict")
  expect_equal(s3$model_name, "my_model")

  s4 <- ds.segmenter.monai_bundle("spleen_ct")
  expect_equal(s4$provider, "monai_bundle_infer")
  expect_equal(s4$bundle_name, "spleen_ct")
})
