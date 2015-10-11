CREATE DATABASE `nytimes_privacy` /*!40100 DEFAULT CHARACTER SET utf8 */;

CREATE TABLE `news_objects` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `object` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `news_details` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `news_object_id` int(11) NOT NULL,
  `web_url` varchar(255) DEFAULT NULL,
  `snippet` text,
  `lead_paragraph` text,
  `abstract` text,
  `print_page` varchar(255) DEFAULT NULL,
  `blog` varchar(255) DEFAULT NULL,
  `source` varchar(255) DEFAULT NULL,
  `multimedia` text,
  `headline` varchar(255) DEFAULT NULL,
  `keywords` text,
  `pub_date` varchar(45) DEFAULT NULL,
  `document_type` varchar(45) DEFAULT NULL,
  `news_desk` varchar(45) DEFAULT NULL,
  `section_name` varchar(45) DEFAULT NULL,
  `subsection_name` varchar(45) DEFAULT NULL,
  `byline` varchar(45) DEFAULT NULL,
  `type_of_material` varchar(45) DEFAULT NULL,
  `_id` varchar(45) DEFAULT NULL,
  `word_count` varchar(45) DEFAULT NULL,
  `slideshow_credits` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_news_object_id_idx` (`news_object_id`),
  CONSTRAINT `fk_news_object_id` FOREIGN KEY (`news_object_id`) REFERENCES `news_objects` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

