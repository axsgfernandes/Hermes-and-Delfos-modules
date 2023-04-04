


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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK
TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users`
VALUES (1, 'ana123@123.com', '$5$rounds=535000$LpbrvkEPaOQzjOpL$z8IJX3Ucgc94lpFJ.KedOKLE3.xJq5M.vNxIr7Gv9jC'),
       (2, '123@123.com', '$5$rounds=535000$HO/uyEh3gc2EWGV0$dLd3Q7/kr.7hoJzIVXNKRTwavKiRsEH/PZEfsS1dmI5'),
       (3, '123@123.com', '$5$rounds=535000$6VIv5ZrMysbHJ4Yb$5IVpNAjgnyGa680ayeIGnhPE1WlQjjWk0AP20a9/cD7'),
       (4, 'ana.xavier.fernandes@gmail.com', '123'),
       (5, 'ola@ola.com', '$5$rounds=535000$IzGT44DBax63utlp$hsKMyXBkE/2dF9lAIQ5UUslrHLuxdnQ9fBnHOpgxHi5'),
       (6, 'ola@cc.com', '$5$rounds=535000$q87tgmvSK9o8GZ16$YQOt4q1OxLttih2.qOpypNplpvu9ob43J2H7tpP/DD7'),
       (7, 'aaa@aaa.com', '$5$rounds=535000$uWSN2/ApAgFt5/La$5QqyPGtY5JPgiE/XG7WJguCSLaKTIFMfIpAwwCLIOJ1'),
       (8, 'oli@oli.com', '$5$rounds=535000$BDmsK4WBatJcbnZl$AEQMGB34TqHXzeeK2L4ZtRiTku/ypX9igMotUP5ZgW6'),
       (9, 'oli2@oli.com', '$5$rounds=535000$BDmsK4WBatJcbnZl$AEQMGB34TqHXzeeK2L4ZtRiTku/ypX9igMotUP5ZgW6'),
       (10, 'user10@gmail.com', '$5$rounds=535000$LpbrvkEPaOQzjOpL$z8IJX3Ucgc94lpFJ.KedOKLE3.xJq5M.vNxIr7Gv9jC');

/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK
TABLES;

-- Dump completed on 2022-02-26 17:08:51
