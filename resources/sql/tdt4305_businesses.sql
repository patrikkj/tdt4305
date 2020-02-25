CREATE TABLE `tdt4305_businesses` (
  `business_id` char(22) NOT NULL,
  `name` tinytext DEFAULT NULL,
  `address` text DEFAULT NULL,
  `city` tinytext DEFAULT NULL,
  `state` char(3) NOT NULL,
  `postal_code` char(10) DEFAULT NULL,
  `latitude` float NOT NULL,
  `longitude` float NOT NULL,
  `location` point NOT NULL,
  `stars` int(11) NOT NULL,
  `review_count` int(11) NOT NULL,
  `categories` text DEFAULT NULL,
  PRIMARY KEY (`business_id`) USING HASH,
);