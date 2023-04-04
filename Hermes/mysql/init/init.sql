use register;
DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users`
(
    `id`       bigint unsigned NOT NULL AUTO_INCREMENT,
    `email`    varchar(100) DEFAULT NULL,
    `password` varchar(300) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 ;