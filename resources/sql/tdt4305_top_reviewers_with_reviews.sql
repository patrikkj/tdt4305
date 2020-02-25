CREATE TABLE `tdt4305_top_reviewers_with_reviews` (
  `review_id` char(22),
  `user_id` char(22) DEFAULT NULL,
  `business_id` char(22) DEFAULT NULL,
  `review_text` text DEFAULT NULL,
  `review_date` datetime(6) DEFAULT NULL,
  KEY `Index 1` (`user_id`) USING HASH
);