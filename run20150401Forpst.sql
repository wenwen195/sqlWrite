add jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar
    /root/esri-git/json-serde-1.3.8-jar-with-dependencies.jar
    /root/esri-git/json-udf-1.3.8-jar-with-dependencies.jar;
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';

DROP TABLE IF EXISTS taxish20150401_Stmax000;
CREATE TABLE taxish20150401_Stmax000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin000;
CREATE TABLE taxish20150401_Stmin000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St000 t GROUP BY t.carId) ts,taxish20150401_St000 t
INSERT OVERWRITE TABLE taxish20150401_Stmax000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St000 t GROUP BY t.carId) ts,taxish20150401_St000 t
INSERT OVERWRITE TABLE taxish20150401_Stmin000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp000;
CREATE TABLE taxish20150401_STODp000(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin000 tmin,taxish20150401_Stmax000 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp000
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf000;
CREATE TABLE taxish20150401_STODf000(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf000
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp000 od1 JOIN taxish20150401_STODp000 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax005;
CREATE TABLE taxish20150401_Stmax005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin005;
CREATE TABLE taxish20150401_Stmin005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St005 t GROUP BY t.carId) ts,taxish20150401_St005 t
INSERT OVERWRITE TABLE taxish20150401_Stmax005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St005 t GROUP BY t.carId) ts,taxish20150401_St005 t
INSERT OVERWRITE TABLE taxish20150401_Stmin005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp005;
CREATE TABLE taxish20150401_STODp005(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin005 tmin,taxish20150401_Stmax005 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp005
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf005;
CREATE TABLE taxish20150401_STODf005(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf005
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp005 od1 JOIN taxish20150401_STODp005 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax010;
CREATE TABLE taxish20150401_Stmax010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin010;
CREATE TABLE taxish20150401_Stmin010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St010 t GROUP BY t.carId) ts,taxish20150401_St010 t
INSERT OVERWRITE TABLE taxish20150401_Stmax010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St010 t GROUP BY t.carId) ts,taxish20150401_St010 t
INSERT OVERWRITE TABLE taxish20150401_Stmin010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp010;
CREATE TABLE taxish20150401_STODp010(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin010 tmin,taxish20150401_Stmax010 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp010
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf010;
CREATE TABLE taxish20150401_STODf010(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf010
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp010 od1 JOIN taxish20150401_STODp010 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax015;
CREATE TABLE taxish20150401_Stmax015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin015;
CREATE TABLE taxish20150401_Stmin015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St015 t GROUP BY t.carId) ts,taxish20150401_St015 t
INSERT OVERWRITE TABLE taxish20150401_Stmax015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St015 t GROUP BY t.carId) ts,taxish20150401_St015 t
INSERT OVERWRITE TABLE taxish20150401_Stmin015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp015;
CREATE TABLE taxish20150401_STODp015(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin015 tmin,taxish20150401_Stmax015 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp015
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf015;
CREATE TABLE taxish20150401_STODf015(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf015
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp015 od1 JOIN taxish20150401_STODp015 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax020;
CREATE TABLE taxish20150401_Stmax020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin020;
CREATE TABLE taxish20150401_Stmin020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St020 t GROUP BY t.carId) ts,taxish20150401_St020 t
INSERT OVERWRITE TABLE taxish20150401_Stmax020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St020 t GROUP BY t.carId) ts,taxish20150401_St020 t
INSERT OVERWRITE TABLE taxish20150401_Stmin020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp020;
CREATE TABLE taxish20150401_STODp020(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin020 tmin,taxish20150401_Stmax020 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp020
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf020;
CREATE TABLE taxish20150401_STODf020(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf020
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp020 od1 JOIN taxish20150401_STODp020 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax025;
CREATE TABLE taxish20150401_Stmax025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin025;
CREATE TABLE taxish20150401_Stmin025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St025 t GROUP BY t.carId) ts,taxish20150401_St025 t
INSERT OVERWRITE TABLE taxish20150401_Stmax025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St025 t GROUP BY t.carId) ts,taxish20150401_St025 t
INSERT OVERWRITE TABLE taxish20150401_Stmin025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp025;
CREATE TABLE taxish20150401_STODp025(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin025 tmin,taxish20150401_Stmax025 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp025
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf025;
CREATE TABLE taxish20150401_STODf025(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf025
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp025 od1 JOIN taxish20150401_STODp025 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax030;
CREATE TABLE taxish20150401_Stmax030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin030;
CREATE TABLE taxish20150401_Stmin030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St030 t GROUP BY t.carId) ts,taxish20150401_St030 t
INSERT OVERWRITE TABLE taxish20150401_Stmax030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St030 t GROUP BY t.carId) ts,taxish20150401_St030 t
INSERT OVERWRITE TABLE taxish20150401_Stmin030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp030;
CREATE TABLE taxish20150401_STODp030(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin030 tmin,taxish20150401_Stmax030 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp030
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf030;
CREATE TABLE taxish20150401_STODf030(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf030
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp030 od1 JOIN taxish20150401_STODp030 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax035;
CREATE TABLE taxish20150401_Stmax035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin035;
CREATE TABLE taxish20150401_Stmin035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St035 t GROUP BY t.carId) ts,taxish20150401_St035 t
INSERT OVERWRITE TABLE taxish20150401_Stmax035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St035 t GROUP BY t.carId) ts,taxish20150401_St035 t
INSERT OVERWRITE TABLE taxish20150401_Stmin035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp035;
CREATE TABLE taxish20150401_STODp035(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin035 tmin,taxish20150401_Stmax035 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp035
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf035;
CREATE TABLE taxish20150401_STODf035(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf035
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp035 od1 JOIN taxish20150401_STODp035 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax040;
CREATE TABLE taxish20150401_Stmax040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin040;
CREATE TABLE taxish20150401_Stmin040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St040 t GROUP BY t.carId) ts,taxish20150401_St040 t
INSERT OVERWRITE TABLE taxish20150401_Stmax040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St040 t GROUP BY t.carId) ts,taxish20150401_St040 t
INSERT OVERWRITE TABLE taxish20150401_Stmin040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp040;
CREATE TABLE taxish20150401_STODp040(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin040 tmin,taxish20150401_Stmax040 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp040
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf040;
CREATE TABLE taxish20150401_STODf040(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf040
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp040 od1 JOIN taxish20150401_STODp040 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax045;
CREATE TABLE taxish20150401_Stmax045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin045;
CREATE TABLE taxish20150401_Stmin045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St045 t GROUP BY t.carId) ts,taxish20150401_St045 t
INSERT OVERWRITE TABLE taxish20150401_Stmax045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St045 t GROUP BY t.carId) ts,taxish20150401_St045 t
INSERT OVERWRITE TABLE taxish20150401_Stmin045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp045;
CREATE TABLE taxish20150401_STODp045(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin045 tmin,taxish20150401_Stmax045 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp045
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf045;
CREATE TABLE taxish20150401_STODf045(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf045
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp045 od1 JOIN taxish20150401_STODp045 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax050;
CREATE TABLE taxish20150401_Stmax050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin050;
CREATE TABLE taxish20150401_Stmin050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St050 t GROUP BY t.carId) ts,taxish20150401_St050 t
INSERT OVERWRITE TABLE taxish20150401_Stmax050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St050 t GROUP BY t.carId) ts,taxish20150401_St050 t
INSERT OVERWRITE TABLE taxish20150401_Stmin050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp050;
CREATE TABLE taxish20150401_STODp050(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin050 tmin,taxish20150401_Stmax050 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp050
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf050;
CREATE TABLE taxish20150401_STODf050(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf050
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp050 od1 JOIN taxish20150401_STODp050 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax055;
CREATE TABLE taxish20150401_Stmax055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin055;
CREATE TABLE taxish20150401_Stmin055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St055 t GROUP BY t.carId) ts,taxish20150401_St055 t
INSERT OVERWRITE TABLE taxish20150401_Stmax055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St055 t GROUP BY t.carId) ts,taxish20150401_St055 t
INSERT OVERWRITE TABLE taxish20150401_Stmin055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp055;
CREATE TABLE taxish20150401_STODp055(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin055 tmin,taxish20150401_Stmax055 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp055
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf055;
CREATE TABLE taxish20150401_STODf055(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf055
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp055 od1 JOIN taxish20150401_STODp055 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax060;
CREATE TABLE taxish20150401_Stmax060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin060;
CREATE TABLE taxish20150401_Stmin060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St060 t GROUP BY t.carId) ts,taxish20150401_St060 t
INSERT OVERWRITE TABLE taxish20150401_Stmax060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St060 t GROUP BY t.carId) ts,taxish20150401_St060 t
INSERT OVERWRITE TABLE taxish20150401_Stmin060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp060;
CREATE TABLE taxish20150401_STODp060(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin060 tmin,taxish20150401_Stmax060 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp060
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf060;
CREATE TABLE taxish20150401_STODf060(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf060
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp060 od1 JOIN taxish20150401_STODp060 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax065;
CREATE TABLE taxish20150401_Stmax065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin065;
CREATE TABLE taxish20150401_Stmin065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St065 t GROUP BY t.carId) ts,taxish20150401_St065 t
INSERT OVERWRITE TABLE taxish20150401_Stmax065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St065 t GROUP BY t.carId) ts,taxish20150401_St065 t
INSERT OVERWRITE TABLE taxish20150401_Stmin065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp065;
CREATE TABLE taxish20150401_STODp065(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin065 tmin,taxish20150401_Stmax065 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp065
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf065;
CREATE TABLE taxish20150401_STODf065(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf065
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp065 od1 JOIN taxish20150401_STODp065 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax070;
CREATE TABLE taxish20150401_Stmax070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin070;
CREATE TABLE taxish20150401_Stmin070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St070 t GROUP BY t.carId) ts,taxish20150401_St070 t
INSERT OVERWRITE TABLE taxish20150401_Stmax070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St070 t GROUP BY t.carId) ts,taxish20150401_St070 t
INSERT OVERWRITE TABLE taxish20150401_Stmin070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp070;
CREATE TABLE taxish20150401_STODp070(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin070 tmin,taxish20150401_Stmax070 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp070
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf070;
CREATE TABLE taxish20150401_STODf070(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf070
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp070 od1 JOIN taxish20150401_STODp070 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax075;
CREATE TABLE taxish20150401_Stmax075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin075;
CREATE TABLE taxish20150401_Stmin075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St075 t GROUP BY t.carId) ts,taxish20150401_St075 t
INSERT OVERWRITE TABLE taxish20150401_Stmax075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St075 t GROUP BY t.carId) ts,taxish20150401_St075 t
INSERT OVERWRITE TABLE taxish20150401_Stmin075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp075;
CREATE TABLE taxish20150401_STODp075(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin075 tmin,taxish20150401_Stmax075 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp075
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf075;
CREATE TABLE taxish20150401_STODf075(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf075
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp075 od1 JOIN taxish20150401_STODp075 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax080;
CREATE TABLE taxish20150401_Stmax080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin080;
CREATE TABLE taxish20150401_Stmin080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St080 t GROUP BY t.carId) ts,taxish20150401_St080 t
INSERT OVERWRITE TABLE taxish20150401_Stmax080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St080 t GROUP BY t.carId) ts,taxish20150401_St080 t
INSERT OVERWRITE TABLE taxish20150401_Stmin080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp080;
CREATE TABLE taxish20150401_STODp080(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin080 tmin,taxish20150401_Stmax080 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp080
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf080;
CREATE TABLE taxish20150401_STODf080(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf080
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp080 od1 JOIN taxish20150401_STODp080 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax085;
CREATE TABLE taxish20150401_Stmax085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin085;
CREATE TABLE taxish20150401_Stmin085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St085 t GROUP BY t.carId) ts,taxish20150401_St085 t
INSERT OVERWRITE TABLE taxish20150401_Stmax085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St085 t GROUP BY t.carId) ts,taxish20150401_St085 t
INSERT OVERWRITE TABLE taxish20150401_Stmin085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp085;
CREATE TABLE taxish20150401_STODp085(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin085 tmin,taxish20150401_Stmax085 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp085
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf085;
CREATE TABLE taxish20150401_STODf085(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf085
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp085 od1 JOIN taxish20150401_STODp085 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax090;
CREATE TABLE taxish20150401_Stmax090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin090;
CREATE TABLE taxish20150401_Stmin090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St090 t GROUP BY t.carId) ts,taxish20150401_St090 t
INSERT OVERWRITE TABLE taxish20150401_Stmax090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St090 t GROUP BY t.carId) ts,taxish20150401_St090 t
INSERT OVERWRITE TABLE taxish20150401_Stmin090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp090;
CREATE TABLE taxish20150401_STODp090(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin090 tmin,taxish20150401_Stmax090 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp090
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf090;
CREATE TABLE taxish20150401_STODf090(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf090
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp090 od1 JOIN taxish20150401_STODp090 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax095;
CREATE TABLE taxish20150401_Stmax095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin095;
CREATE TABLE taxish20150401_Stmin095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St095 t GROUP BY t.carId) ts,taxish20150401_St095 t
INSERT OVERWRITE TABLE taxish20150401_Stmax095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St095 t GROUP BY t.carId) ts,taxish20150401_St095 t
INSERT OVERWRITE TABLE taxish20150401_Stmin095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp095;
CREATE TABLE taxish20150401_STODp095(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin095 tmin,taxish20150401_Stmax095 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp095
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf095;
CREATE TABLE taxish20150401_STODf095(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf095
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp095 od1 JOIN taxish20150401_STODp095 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax100;
CREATE TABLE taxish20150401_Stmax100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin100;
CREATE TABLE taxish20150401_Stmin100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St100 t GROUP BY t.carId) ts,taxish20150401_St100 t
INSERT OVERWRITE TABLE taxish20150401_Stmax100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St100 t GROUP BY t.carId) ts,taxish20150401_St100 t
INSERT OVERWRITE TABLE taxish20150401_Stmin100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp100;
CREATE TABLE taxish20150401_STODp100(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin100 tmin,taxish20150401_Stmax100 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp100
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf100;
CREATE TABLE taxish20150401_STODf100(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf100
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp100 od1 JOIN taxish20150401_STODp100 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax105;
CREATE TABLE taxish20150401_Stmax105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin105;
CREATE TABLE taxish20150401_Stmin105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St105 t GROUP BY t.carId) ts,taxish20150401_St105 t
INSERT OVERWRITE TABLE taxish20150401_Stmax105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St105 t GROUP BY t.carId) ts,taxish20150401_St105 t
INSERT OVERWRITE TABLE taxish20150401_Stmin105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp105;
CREATE TABLE taxish20150401_STODp105(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin105 tmin,taxish20150401_Stmax105 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp105
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf105;
CREATE TABLE taxish20150401_STODf105(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf105
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp105 od1 JOIN taxish20150401_STODp105 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax110;
CREATE TABLE taxish20150401_Stmax110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin110;
CREATE TABLE taxish20150401_Stmin110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St110 t GROUP BY t.carId) ts,taxish20150401_St110 t
INSERT OVERWRITE TABLE taxish20150401_Stmax110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St110 t GROUP BY t.carId) ts,taxish20150401_St110 t
INSERT OVERWRITE TABLE taxish20150401_Stmin110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp110;
CREATE TABLE taxish20150401_STODp110(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin110 tmin,taxish20150401_Stmax110 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp110
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf110;
CREATE TABLE taxish20150401_STODf110(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf110
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp110 od1 JOIN taxish20150401_STODp110 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax115;
CREATE TABLE taxish20150401_Stmax115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin115;
CREATE TABLE taxish20150401_Stmin115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St115 t GROUP BY t.carId) ts,taxish20150401_St115 t
INSERT OVERWRITE TABLE taxish20150401_Stmax115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St115 t GROUP BY t.carId) ts,taxish20150401_St115 t
INSERT OVERWRITE TABLE taxish20150401_Stmin115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp115;
CREATE TABLE taxish20150401_STODp115(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin115 tmin,taxish20150401_Stmax115 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp115
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf115;
CREATE TABLE taxish20150401_STODf115(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf115
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp115 od1 JOIN taxish20150401_STODp115 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax120;
CREATE TABLE taxish20150401_Stmax120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin120;
CREATE TABLE taxish20150401_Stmin120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St120 t GROUP BY t.carId) ts,taxish20150401_St120 t
INSERT OVERWRITE TABLE taxish20150401_Stmax120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St120 t GROUP BY t.carId) ts,taxish20150401_St120 t
INSERT OVERWRITE TABLE taxish20150401_Stmin120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp120;
CREATE TABLE taxish20150401_STODp120(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin120 tmin,taxish20150401_Stmax120 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp120
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf120;
CREATE TABLE taxish20150401_STODf120(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf120
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp120 od1 JOIN taxish20150401_STODp120 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax125;
CREATE TABLE taxish20150401_Stmax125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin125;
CREATE TABLE taxish20150401_Stmin125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St125 t GROUP BY t.carId) ts,taxish20150401_St125 t
INSERT OVERWRITE TABLE taxish20150401_Stmax125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St125 t GROUP BY t.carId) ts,taxish20150401_St125 t
INSERT OVERWRITE TABLE taxish20150401_Stmin125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp125;
CREATE TABLE taxish20150401_STODp125(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin125 tmin,taxish20150401_Stmax125 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp125
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf125;
CREATE TABLE taxish20150401_STODf125(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf125
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp125 od1 JOIN taxish20150401_STODp125 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax130;
CREATE TABLE taxish20150401_Stmax130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin130;
CREATE TABLE taxish20150401_Stmin130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St130 t GROUP BY t.carId) ts,taxish20150401_St130 t
INSERT OVERWRITE TABLE taxish20150401_Stmax130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St130 t GROUP BY t.carId) ts,taxish20150401_St130 t
INSERT OVERWRITE TABLE taxish20150401_Stmin130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp130;
CREATE TABLE taxish20150401_STODp130(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin130 tmin,taxish20150401_Stmax130 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp130
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf130;
CREATE TABLE taxish20150401_STODf130(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf130
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp130 od1 JOIN taxish20150401_STODp130 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax135;
CREATE TABLE taxish20150401_Stmax135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin135;
CREATE TABLE taxish20150401_Stmin135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St135 t GROUP BY t.carId) ts,taxish20150401_St135 t
INSERT OVERWRITE TABLE taxish20150401_Stmax135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St135 t GROUP BY t.carId) ts,taxish20150401_St135 t
INSERT OVERWRITE TABLE taxish20150401_Stmin135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp135;
CREATE TABLE taxish20150401_STODp135(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin135 tmin,taxish20150401_Stmax135 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp135
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf135;
CREATE TABLE taxish20150401_STODf135(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf135
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp135 od1 JOIN taxish20150401_STODp135 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax140;
CREATE TABLE taxish20150401_Stmax140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin140;
CREATE TABLE taxish20150401_Stmin140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St140 t GROUP BY t.carId) ts,taxish20150401_St140 t
INSERT OVERWRITE TABLE taxish20150401_Stmax140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St140 t GROUP BY t.carId) ts,taxish20150401_St140 t
INSERT OVERWRITE TABLE taxish20150401_Stmin140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp140;
CREATE TABLE taxish20150401_STODp140(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin140 tmin,taxish20150401_Stmax140 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp140
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf140;
CREATE TABLE taxish20150401_STODf140(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf140
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp140 od1 JOIN taxish20150401_STODp140 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax145;
CREATE TABLE taxish20150401_Stmax145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin145;
CREATE TABLE taxish20150401_Stmin145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St145 t GROUP BY t.carId) ts,taxish20150401_St145 t
INSERT OVERWRITE TABLE taxish20150401_Stmax145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St145 t GROUP BY t.carId) ts,taxish20150401_St145 t
INSERT OVERWRITE TABLE taxish20150401_Stmin145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp145;
CREATE TABLE taxish20150401_STODp145(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin145 tmin,taxish20150401_Stmax145 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp145
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf145;
CREATE TABLE taxish20150401_STODf145(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf145
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp145 od1 JOIN taxish20150401_STODp145 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax150;
CREATE TABLE taxish20150401_Stmax150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin150;
CREATE TABLE taxish20150401_Stmin150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St150 t GROUP BY t.carId) ts,taxish20150401_St150 t
INSERT OVERWRITE TABLE taxish20150401_Stmax150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St150 t GROUP BY t.carId) ts,taxish20150401_St150 t
INSERT OVERWRITE TABLE taxish20150401_Stmin150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp150;
CREATE TABLE taxish20150401_STODp150(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin150 tmin,taxish20150401_Stmax150 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp150
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf150;
CREATE TABLE taxish20150401_STODf150(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf150
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp150 od1 JOIN taxish20150401_STODp150 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax155;
CREATE TABLE taxish20150401_Stmax155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin155;
CREATE TABLE taxish20150401_Stmin155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St155 t GROUP BY t.carId) ts,taxish20150401_St155 t
INSERT OVERWRITE TABLE taxish20150401_Stmax155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St155 t GROUP BY t.carId) ts,taxish20150401_St155 t
INSERT OVERWRITE TABLE taxish20150401_Stmin155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp155;
CREATE TABLE taxish20150401_STODp155(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin155 tmin,taxish20150401_Stmax155 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp155
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf155;
CREATE TABLE taxish20150401_STODf155(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf155
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp155 od1 JOIN taxish20150401_STODp155 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax160;
CREATE TABLE taxish20150401_Stmax160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin160;
CREATE TABLE taxish20150401_Stmin160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St160 t GROUP BY t.carId) ts,taxish20150401_St160 t
INSERT OVERWRITE TABLE taxish20150401_Stmax160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St160 t GROUP BY t.carId) ts,taxish20150401_St160 t
INSERT OVERWRITE TABLE taxish20150401_Stmin160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp160;
CREATE TABLE taxish20150401_STODp160(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin160 tmin,taxish20150401_Stmax160 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp160
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf160;
CREATE TABLE taxish20150401_STODf160(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf160
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp160 od1 JOIN taxish20150401_STODp160 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax165;
CREATE TABLE taxish20150401_Stmax165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin165;
CREATE TABLE taxish20150401_Stmin165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St165 t GROUP BY t.carId) ts,taxish20150401_St165 t
INSERT OVERWRITE TABLE taxish20150401_Stmax165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St165 t GROUP BY t.carId) ts,taxish20150401_St165 t
INSERT OVERWRITE TABLE taxish20150401_Stmin165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp165;
CREATE TABLE taxish20150401_STODp165(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin165 tmin,taxish20150401_Stmax165 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp165
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf165;
CREATE TABLE taxish20150401_STODf165(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf165
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp165 od1 JOIN taxish20150401_STODp165 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax170;
CREATE TABLE taxish20150401_Stmax170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin170;
CREATE TABLE taxish20150401_Stmin170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St170 t GROUP BY t.carId) ts,taxish20150401_St170 t
INSERT OVERWRITE TABLE taxish20150401_Stmax170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St170 t GROUP BY t.carId) ts,taxish20150401_St170 t
INSERT OVERWRITE TABLE taxish20150401_Stmin170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp170;
CREATE TABLE taxish20150401_STODp170(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin170 tmin,taxish20150401_Stmax170 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp170
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf170;
CREATE TABLE taxish20150401_STODf170(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf170
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp170 od1 JOIN taxish20150401_STODp170 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax175;
CREATE TABLE taxish20150401_Stmax175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin175;
CREATE TABLE taxish20150401_Stmin175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St175 t GROUP BY t.carId) ts,taxish20150401_St175 t
INSERT OVERWRITE TABLE taxish20150401_Stmax175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St175 t GROUP BY t.carId) ts,taxish20150401_St175 t
INSERT OVERWRITE TABLE taxish20150401_Stmin175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp175;
CREATE TABLE taxish20150401_STODp175(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin175 tmin,taxish20150401_Stmax175 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp175
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf175;
CREATE TABLE taxish20150401_STODf175(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf175
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp175 od1 JOIN taxish20150401_STODp175 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax180;
CREATE TABLE taxish20150401_Stmax180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin180;
CREATE TABLE taxish20150401_Stmin180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St180 t GROUP BY t.carId) ts,taxish20150401_St180 t
INSERT OVERWRITE TABLE taxish20150401_Stmax180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St180 t GROUP BY t.carId) ts,taxish20150401_St180 t
INSERT OVERWRITE TABLE taxish20150401_Stmin180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp180;
CREATE TABLE taxish20150401_STODp180(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin180 tmin,taxish20150401_Stmax180 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp180
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf180;
CREATE TABLE taxish20150401_STODf180(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf180
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp180 od1 JOIN taxish20150401_STODp180 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax185;
CREATE TABLE taxish20150401_Stmax185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin185;
CREATE TABLE taxish20150401_Stmin185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmax185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmin185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp185;
CREATE TABLE taxish20150401_STODp185(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin185 tmin,taxish20150401_Stmax185 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp185
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf185;
CREATE TABLE taxish20150401_STODf185(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf185
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp185 od1 JOIN taxish20150401_STODp185 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax190;
CREATE TABLE taxish20150401_Stmax190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin190;
CREATE TABLE taxish20150401_Stmin190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St190 t GROUP BY t.carId) ts,taxish20150401_St190 t
INSERT OVERWRITE TABLE taxish20150401_Stmax190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St190 t GROUP BY t.carId) ts,taxish20150401_St190 t
INSERT OVERWRITE TABLE taxish20150401_Stmin190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp190;
CREATE TABLE taxish20150401_STODp190(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin190 tmin,taxish20150401_Stmax190 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp190
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf190;
CREATE TABLE taxish20150401_STODf190(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf190
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp190 od1 JOIN taxish20150401_STODp190 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax195;
CREATE TABLE taxish20150401_Stmax195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin195;
CREATE TABLE taxish20150401_Stmin195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St195 t GROUP BY t.carId) ts,taxish20150401_St195 t
INSERT OVERWRITE TABLE taxish20150401_Stmax195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St195 t GROUP BY t.carId) ts,taxish20150401_St195 t
INSERT OVERWRITE TABLE taxish20150401_Stmin195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp195;
CREATE TABLE taxish20150401_STODp195(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin195 tmin,taxish20150401_Stmax195 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp195
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf195;
CREATE TABLE taxish20150401_STODf195(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf195
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp195 od1 JOIN taxish20150401_STODp195 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax200;
CREATE TABLE taxish20150401_Stmax200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin200;
CREATE TABLE taxish20150401_Stmin200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St200 t GROUP BY t.carId) ts,taxish20150401_St200 t
INSERT OVERWRITE TABLE taxish20150401_Stmax200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St200 t GROUP BY t.carId) ts,taxish20150401_St200 t
INSERT OVERWRITE TABLE taxish20150401_Stmin200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp200;
CREATE TABLE taxish20150401_STODp200(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin200 tmin,taxish20150401_Stmax200 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp200
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf200;
CREATE TABLE taxish20150401_STODf200(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf200
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp200 od1 JOIN taxish20150401_STODp200 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax205;
CREATE TABLE taxish20150401_Stmax205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin205;
CREATE TABLE taxish20150401_Stmin205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St205 t GROUP BY t.carId) ts,taxish20150401_St205 t
INSERT OVERWRITE TABLE taxish20150401_Stmax205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St205 t GROUP BY t.carId) ts,taxish20150401_St205 t
INSERT OVERWRITE TABLE taxish20150401_Stmin205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp205;
CREATE TABLE taxish20150401_STODp205(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin205 tmin,taxish20150401_Stmax205 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp205
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf205;
CREATE TABLE taxish20150401_STODf205(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf205
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp205 od1 JOIN taxish20150401_STODp205 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax210;
CREATE TABLE taxish20150401_Stmax210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin210;
CREATE TABLE taxish20150401_Stmin210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St210 t GROUP BY t.carId) ts,taxish20150401_St210 t
INSERT OVERWRITE TABLE taxish20150401_Stmax210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St210 t GROUP BY t.carId) ts,taxish20150401_St210 t
INSERT OVERWRITE TABLE taxish20150401_Stmin210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp210;
CREATE TABLE taxish20150401_STODp210(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin210 tmin,taxish20150401_Stmax210 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp210
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf210;
CREATE TABLE taxish20150401_STODf210(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf210
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp210 od1 JOIN taxish20150401_STODp210 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax215;
CREATE TABLE taxish20150401_Stmax215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin215;
CREATE TABLE taxish20150401_Stmin215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St215 t GROUP BY t.carId) ts,taxish20150401_St215 t
INSERT OVERWRITE TABLE taxish20150401_Stmax215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St215 t GROUP BY t.carId) ts,taxish20150401_St215 t
INSERT OVERWRITE TABLE taxish20150401_Stmin215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp215;
CREATE TABLE taxish20150401_STODp215(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin215 tmin,taxish20150401_Stmax215 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp215
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf215;
CREATE TABLE taxish20150401_STODf215(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf215
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp215 od1 JOIN taxish20150401_STODp215 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax220;
CREATE TABLE taxish20150401_Stmax220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin220;
CREATE TABLE taxish20150401_Stmin220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St220 t GROUP BY t.carId) ts,taxish20150401_St220 t
INSERT OVERWRITE TABLE taxish20150401_Stmax220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St220 t GROUP BY t.carId) ts,taxish20150401_St220 t
INSERT OVERWRITE TABLE taxish20150401_Stmin220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp220;
CREATE TABLE taxish20150401_STODp220(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin220 tmin,taxish20150401_Stmax220 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp220
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf220;
CREATE TABLE taxish20150401_STODf220(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf220
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp220 od1 JOIN taxish20150401_STODp220 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax225;
CREATE TABLE taxish20150401_Stmax225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin225;
CREATE TABLE taxish20150401_Stmin225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St225 t GROUP BY t.carId) ts,taxish20150401_St225 t
INSERT OVERWRITE TABLE taxish20150401_Stmax225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St225 t GROUP BY t.carId) ts,taxish20150401_St225 t
INSERT OVERWRITE TABLE taxish20150401_Stmin225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp225;
CREATE TABLE taxish20150401_STODp225(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin225 tmin,taxish20150401_Stmax225 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp225
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf225;
CREATE TABLE taxish20150401_STODf225(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf225
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp225 od1 JOIN taxish20150401_STODp225 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax230;
CREATE TABLE taxish20150401_Stmax230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin230;
CREATE TABLE taxish20150401_Stmin230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St230 t GROUP BY t.carId) ts,taxish20150401_St230 t
INSERT OVERWRITE TABLE taxish20150401_Stmax230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St230 t GROUP BY t.carId) ts,taxish20150401_St230 t
INSERT OVERWRITE TABLE taxish20150401_Stmin230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp230;
CREATE TABLE taxish20150401_STODp230(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin230 tmin,taxish20150401_Stmax230 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp230
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf230;
CREATE TABLE taxish20150401_STODf230(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf230
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp230 od1 JOIN taxish20150401_STODp230 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_Stmax235;
CREATE TABLE taxish20150401_Stmax235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_Stmin235;
CREATE TABLE taxish20150401_Stmin235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St235 t GROUP BY t.carId) ts,taxish20150401_St235 t
INSERT OVERWRITE TABLE taxish20150401_Stmax235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St235 t GROUP BY t.carId) ts,taxish20150401_St235 t
INSERT OVERWRITE TABLE taxish20150401_Stmin235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp235;
CREATE TABLE taxish20150401_STODp235(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM taxish20150401_Stmin235 tmin,taxish20150401_Stmax235 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp235
SELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STODf235;
CREATE TABLE taxish20150401_STODf235(OctOBJECTID int,DctOBJECTID int,count DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STODf235
SELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count
FROM taxish20150401_STODp235 od1 JOIN taxish20150401_STODp235 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
