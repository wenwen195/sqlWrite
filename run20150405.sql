add jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar
    /root/esri-git/json-serde-1.3.8-jar-with-dependencies.jar
    /root/esri-git/json-udf-1.3.8-jar-with-dependencies.jar;
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';

DROP TABLE IF EXISTS taxish20150405_;
CREATE EXTERNAL TABLE taxish20150405_(carId DOUBLE,isAlarm DOUBLE,isEmpty DOUBLE,topLight DOUBLE,
Elevated DOUBLE,isBrake DOUBLE,receiveTime TIMESTAMP,GPSTime STRING,longitude DOUBLE,latitude DOUBLE,
speed DOUBLE,direction DOUBLE,satellite DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile
tblproperties ("skip.header.line.count"="1");
describe taxish20150405_;
LOAD DATA LOCAL INPATH '/home/part-4.05.csv' OVERWRITE INTO TABLE taxish20150405_;

DROP TABLE IF EXISTS taxish20150405_value;
CREATE EXTERNAL TABLE taxish20150405_value(p STRING,m STRING,n DOUBLE,x DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
DROP TABLE IF EXISTS taxish20150405_valuepp;
CREATE TABLE taxish20150405_valuepp(time STRING,c DOUBLE);

DROP TABLE IF EXISTS taxish20150405_time000;
CREATE TABLE taxish20150405_time000(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 00:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time000
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 00:30:00';

DROP TABLE IF EXISTS taxish20150405_St000;
CREATE EXTERNAL TABLE taxish20150405_St000(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St000
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time000 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time000;
DROP TABLE IF EXISTS taxish20150405_Stmax000;
CREATE TABLE taxish20150405_Stmax000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin000;
CREATE TABLE taxish20150405_Stmin000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St000 t GROUP BY t.carId) ts,taxish20150405_St000 t
INSERT OVERWRITE TABLE taxish20150405_Stmax000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St000 t GROUP BY t.carId) ts,taxish20150405_St000 t
INSERT OVERWRITE TABLE taxish20150405_Stmin000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp000;
CREATE TABLE taxish20150405_STODp000(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin000 tmin,taxish20150405_Stmax000 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp000
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf000;
CREATE TABLE taxish20150405_STODf000(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf000
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp000 od1 JOIN taxish20150405_STODp000 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD000;
CREATE TABLE taxish20150405_STOD000(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD000
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf000
WHERE count>0;
drop table taxish20150405_Stmax000;
drop table taxish20150405_Stmin000;
drop table taxish20150405_STODf000;
FROM taxish20150405_STOD000
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","000",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO000;
CREATE TABLE taxish20150405_STO000(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD000;
CREATE TABLE taxish20150405_STD000(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO000
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp000
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD000
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp000
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP000;
CREATE TABLE taxish20150405_STTP000(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO000 o,taxish20150405_STD000 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP000
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO000;
drop table taxish20150405_STD000;
drop table taxish20150405_STODp000;
DROP TABLE IF EXISTS taxish20150405_STTPn000;
CREATE TABLE taxish20150405_STTPn000(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP000 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn000
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg000;
CREATE TABLE taxish20150405_stagg000(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg000
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St000 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn000;
CREATE TABLE taxish20150405_staggn000(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn000
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St000 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1000;
CREATE TABLE taxish20150405_agg1000(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St000) bins
INSERT OVERWRITE TABLE taxish20150405_agg1000
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2000;
CREATE TABLE taxish20150405_agg2000(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St000) bins
INSERT OVERWRITE TABLE taxish20150405_agg2000
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1000
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","000",MIN(c),MAX(c);
FROM taxish20150405_agg2000
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","000",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time005;
CREATE TABLE taxish20150405_time005(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 00:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time005
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 01:00:00';

DROP TABLE IF EXISTS taxish20150405_St005;
CREATE EXTERNAL TABLE taxish20150405_St005(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St005
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time005 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time005;
DROP TABLE IF EXISTS taxish20150405_Stmax005;
CREATE TABLE taxish20150405_Stmax005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin005;
CREATE TABLE taxish20150405_Stmin005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St005 t GROUP BY t.carId) ts,taxish20150405_St005 t
INSERT OVERWRITE TABLE taxish20150405_Stmax005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St005 t GROUP BY t.carId) ts,taxish20150405_St005 t
INSERT OVERWRITE TABLE taxish20150405_Stmin005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp005;
CREATE TABLE taxish20150405_STODp005(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin005 tmin,taxish20150405_Stmax005 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp005
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf005;
CREATE TABLE taxish20150405_STODf005(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf005
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp005 od1 JOIN taxish20150405_STODp005 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD005;
CREATE TABLE taxish20150405_STOD005(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD005
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf005
WHERE count>0;
drop table taxish20150405_Stmax005;
drop table taxish20150405_Stmin005;
drop table taxish20150405_STODf005;
FROM taxish20150405_STOD005
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","005",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO005;
CREATE TABLE taxish20150405_STO005(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD005;
CREATE TABLE taxish20150405_STD005(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO005
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp005
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD005
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp005
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP005;
CREATE TABLE taxish20150405_STTP005(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO005 o,taxish20150405_STD005 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP005
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO005;
drop table taxish20150405_STD005;
drop table taxish20150405_STODp005;
DROP TABLE IF EXISTS taxish20150405_STTPn005;
CREATE TABLE taxish20150405_STTPn005(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP005 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn005
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg005;
CREATE TABLE taxish20150405_stagg005(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg005
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St005 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn005;
CREATE TABLE taxish20150405_staggn005(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn005
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St005 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1005;
CREATE TABLE taxish20150405_agg1005(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St005) bins
INSERT OVERWRITE TABLE taxish20150405_agg1005
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2005;
CREATE TABLE taxish20150405_agg2005(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St005) bins
INSERT OVERWRITE TABLE taxish20150405_agg2005
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1005
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","005",MIN(c),MAX(c);
FROM taxish20150405_agg2005
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","005",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time010;
CREATE TABLE taxish20150405_time010(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 01:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time010
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 01:30:00';

DROP TABLE IF EXISTS taxish20150405_St010;
CREATE EXTERNAL TABLE taxish20150405_St010(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St010
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time010 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time010;
DROP TABLE IF EXISTS taxish20150405_Stmax010;
CREATE TABLE taxish20150405_Stmax010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin010;
CREATE TABLE taxish20150405_Stmin010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St010 t GROUP BY t.carId) ts,taxish20150405_St010 t
INSERT OVERWRITE TABLE taxish20150405_Stmax010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St010 t GROUP BY t.carId) ts,taxish20150405_St010 t
INSERT OVERWRITE TABLE taxish20150405_Stmin010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp010;
CREATE TABLE taxish20150405_STODp010(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin010 tmin,taxish20150405_Stmax010 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp010
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf010;
CREATE TABLE taxish20150405_STODf010(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf010
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp010 od1 JOIN taxish20150405_STODp010 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD010;
CREATE TABLE taxish20150405_STOD010(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD010
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf010
WHERE count>0;
drop table taxish20150405_Stmax010;
drop table taxish20150405_Stmin010;
drop table taxish20150405_STODf010;
FROM taxish20150405_STOD010
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","010",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO010;
CREATE TABLE taxish20150405_STO010(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD010;
CREATE TABLE taxish20150405_STD010(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO010
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp010
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD010
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp010
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP010;
CREATE TABLE taxish20150405_STTP010(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO010 o,taxish20150405_STD010 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP010
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO010;
drop table taxish20150405_STD010;
drop table taxish20150405_STODp010;
DROP TABLE IF EXISTS taxish20150405_STTPn010;
CREATE TABLE taxish20150405_STTPn010(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP010 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn010
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg010;
CREATE TABLE taxish20150405_stagg010(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg010
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St010 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn010;
CREATE TABLE taxish20150405_staggn010(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn010
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St010 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1010;
CREATE TABLE taxish20150405_agg1010(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St010) bins
INSERT OVERWRITE TABLE taxish20150405_agg1010
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2010;
CREATE TABLE taxish20150405_agg2010(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St010) bins
INSERT OVERWRITE TABLE taxish20150405_agg2010
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1010
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","010",MIN(c),MAX(c);
FROM taxish20150405_agg2010
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","010",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time015;
CREATE TABLE taxish20150405_time015(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 01:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time015
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 02:00:00';

DROP TABLE IF EXISTS taxish20150405_St015;
CREATE EXTERNAL TABLE taxish20150405_St015(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St015
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time015 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time015;
DROP TABLE IF EXISTS taxish20150405_Stmax015;
CREATE TABLE taxish20150405_Stmax015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin015;
CREATE TABLE taxish20150405_Stmin015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St015 t GROUP BY t.carId) ts,taxish20150405_St015 t
INSERT OVERWRITE TABLE taxish20150405_Stmax015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St015 t GROUP BY t.carId) ts,taxish20150405_St015 t
INSERT OVERWRITE TABLE taxish20150405_Stmin015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp015;
CREATE TABLE taxish20150405_STODp015(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin015 tmin,taxish20150405_Stmax015 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp015
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf015;
CREATE TABLE taxish20150405_STODf015(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf015
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp015 od1 JOIN taxish20150405_STODp015 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD015;
CREATE TABLE taxish20150405_STOD015(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD015
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf015
WHERE count>0;
drop table taxish20150405_Stmax015;
drop table taxish20150405_Stmin015;
drop table taxish20150405_STODf015;
FROM taxish20150405_STOD015
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","015",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO015;
CREATE TABLE taxish20150405_STO015(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD015;
CREATE TABLE taxish20150405_STD015(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO015
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp015
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD015
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp015
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP015;
CREATE TABLE taxish20150405_STTP015(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO015 o,taxish20150405_STD015 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP015
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO015;
drop table taxish20150405_STD015;
drop table taxish20150405_STODp015;
DROP TABLE IF EXISTS taxish20150405_STTPn015;
CREATE TABLE taxish20150405_STTPn015(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP015 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn015
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg015;
CREATE TABLE taxish20150405_stagg015(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg015
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St015 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn015;
CREATE TABLE taxish20150405_staggn015(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn015
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St015 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1015;
CREATE TABLE taxish20150405_agg1015(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St015) bins
INSERT OVERWRITE TABLE taxish20150405_agg1015
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2015;
CREATE TABLE taxish20150405_agg2015(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St015) bins
INSERT OVERWRITE TABLE taxish20150405_agg2015
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1015
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","015",MIN(c),MAX(c);
FROM taxish20150405_agg2015
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","015",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time020;
CREATE TABLE taxish20150405_time020(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 02:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time020
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 02:30:00';

DROP TABLE IF EXISTS taxish20150405_St020;
CREATE EXTERNAL TABLE taxish20150405_St020(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St020
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time020 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time020;
DROP TABLE IF EXISTS taxish20150405_Stmax020;
CREATE TABLE taxish20150405_Stmax020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin020;
CREATE TABLE taxish20150405_Stmin020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St020 t GROUP BY t.carId) ts,taxish20150405_St020 t
INSERT OVERWRITE TABLE taxish20150405_Stmax020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St020 t GROUP BY t.carId) ts,taxish20150405_St020 t
INSERT OVERWRITE TABLE taxish20150405_Stmin020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp020;
CREATE TABLE taxish20150405_STODp020(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin020 tmin,taxish20150405_Stmax020 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp020
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf020;
CREATE TABLE taxish20150405_STODf020(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf020
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp020 od1 JOIN taxish20150405_STODp020 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD020;
CREATE TABLE taxish20150405_STOD020(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD020
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf020
WHERE count>0;
drop table taxish20150405_Stmax020;
drop table taxish20150405_Stmin020;
drop table taxish20150405_STODf020;
FROM taxish20150405_STOD020
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","020",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO020;
CREATE TABLE taxish20150405_STO020(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD020;
CREATE TABLE taxish20150405_STD020(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO020
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp020
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD020
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp020
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP020;
CREATE TABLE taxish20150405_STTP020(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO020 o,taxish20150405_STD020 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP020
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO020;
drop table taxish20150405_STD020;
drop table taxish20150405_STODp020;
DROP TABLE IF EXISTS taxish20150405_STTPn020;
CREATE TABLE taxish20150405_STTPn020(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP020 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn020
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg020;
CREATE TABLE taxish20150405_stagg020(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg020
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St020 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn020;
CREATE TABLE taxish20150405_staggn020(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn020
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St020 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1020;
CREATE TABLE taxish20150405_agg1020(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St020) bins
INSERT OVERWRITE TABLE taxish20150405_agg1020
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2020;
CREATE TABLE taxish20150405_agg2020(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St020) bins
INSERT OVERWRITE TABLE taxish20150405_agg2020
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1020
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","020",MIN(c),MAX(c);
FROM taxish20150405_agg2020
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","020",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time025;
CREATE TABLE taxish20150405_time025(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 02:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time025
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 03:00:00';

DROP TABLE IF EXISTS taxish20150405_St025;
CREATE EXTERNAL TABLE taxish20150405_St025(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St025
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time025 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time025;
DROP TABLE IF EXISTS taxish20150405_Stmax025;
CREATE TABLE taxish20150405_Stmax025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin025;
CREATE TABLE taxish20150405_Stmin025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St025 t GROUP BY t.carId) ts,taxish20150405_St025 t
INSERT OVERWRITE TABLE taxish20150405_Stmax025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St025 t GROUP BY t.carId) ts,taxish20150405_St025 t
INSERT OVERWRITE TABLE taxish20150405_Stmin025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp025;
CREATE TABLE taxish20150405_STODp025(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin025 tmin,taxish20150405_Stmax025 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp025
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf025;
CREATE TABLE taxish20150405_STODf025(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf025
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp025 od1 JOIN taxish20150405_STODp025 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD025;
CREATE TABLE taxish20150405_STOD025(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD025
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf025
WHERE count>0;
drop table taxish20150405_Stmax025;
drop table taxish20150405_Stmin025;
drop table taxish20150405_STODf025;
FROM taxish20150405_STOD025
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","025",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO025;
CREATE TABLE taxish20150405_STO025(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD025;
CREATE TABLE taxish20150405_STD025(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO025
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp025
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD025
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp025
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP025;
CREATE TABLE taxish20150405_STTP025(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO025 o,taxish20150405_STD025 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP025
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO025;
drop table taxish20150405_STD025;
drop table taxish20150405_STODp025;
DROP TABLE IF EXISTS taxish20150405_STTPn025;
CREATE TABLE taxish20150405_STTPn025(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP025 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn025
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg025;
CREATE TABLE taxish20150405_stagg025(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg025
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St025 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn025;
CREATE TABLE taxish20150405_staggn025(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn025
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St025 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1025;
CREATE TABLE taxish20150405_agg1025(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St025) bins
INSERT OVERWRITE TABLE taxish20150405_agg1025
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2025;
CREATE TABLE taxish20150405_agg2025(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St025) bins
INSERT OVERWRITE TABLE taxish20150405_agg2025
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1025
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","025",MIN(c),MAX(c);
FROM taxish20150405_agg2025
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","025",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time030;
CREATE TABLE taxish20150405_time030(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 03:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time030
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 03:30:00';

DROP TABLE IF EXISTS taxish20150405_St030;
CREATE EXTERNAL TABLE taxish20150405_St030(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St030
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time030 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time030;
DROP TABLE IF EXISTS taxish20150405_Stmax030;
CREATE TABLE taxish20150405_Stmax030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin030;
CREATE TABLE taxish20150405_Stmin030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St030 t GROUP BY t.carId) ts,taxish20150405_St030 t
INSERT OVERWRITE TABLE taxish20150405_Stmax030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St030 t GROUP BY t.carId) ts,taxish20150405_St030 t
INSERT OVERWRITE TABLE taxish20150405_Stmin030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp030;
CREATE TABLE taxish20150405_STODp030(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin030 tmin,taxish20150405_Stmax030 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp030
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf030;
CREATE TABLE taxish20150405_STODf030(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf030
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp030 od1 JOIN taxish20150405_STODp030 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD030;
CREATE TABLE taxish20150405_STOD030(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD030
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf030
WHERE count>0;
drop table taxish20150405_Stmax030;
drop table taxish20150405_Stmin030;
drop table taxish20150405_STODf030;
FROM taxish20150405_STOD030
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","030",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO030;
CREATE TABLE taxish20150405_STO030(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD030;
CREATE TABLE taxish20150405_STD030(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO030
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp030
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD030
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp030
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP030;
CREATE TABLE taxish20150405_STTP030(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO030 o,taxish20150405_STD030 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP030
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO030;
drop table taxish20150405_STD030;
drop table taxish20150405_STODp030;
DROP TABLE IF EXISTS taxish20150405_STTPn030;
CREATE TABLE taxish20150405_STTPn030(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP030 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn030
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg030;
CREATE TABLE taxish20150405_stagg030(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg030
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St030 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn030;
CREATE TABLE taxish20150405_staggn030(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn030
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St030 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1030;
CREATE TABLE taxish20150405_agg1030(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St030) bins
INSERT OVERWRITE TABLE taxish20150405_agg1030
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2030;
CREATE TABLE taxish20150405_agg2030(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St030) bins
INSERT OVERWRITE TABLE taxish20150405_agg2030
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1030
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","030",MIN(c),MAX(c);
FROM taxish20150405_agg2030
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","030",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time035;
CREATE TABLE taxish20150405_time035(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 03:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time035
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 04:00:00';

DROP TABLE IF EXISTS taxish20150405_St035;
CREATE EXTERNAL TABLE taxish20150405_St035(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St035
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time035 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time035;
DROP TABLE IF EXISTS taxish20150405_Stmax035;
CREATE TABLE taxish20150405_Stmax035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin035;
CREATE TABLE taxish20150405_Stmin035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St035 t GROUP BY t.carId) ts,taxish20150405_St035 t
INSERT OVERWRITE TABLE taxish20150405_Stmax035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St035 t GROUP BY t.carId) ts,taxish20150405_St035 t
INSERT OVERWRITE TABLE taxish20150405_Stmin035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp035;
CREATE TABLE taxish20150405_STODp035(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin035 tmin,taxish20150405_Stmax035 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp035
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf035;
CREATE TABLE taxish20150405_STODf035(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf035
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp035 od1 JOIN taxish20150405_STODp035 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD035;
CREATE TABLE taxish20150405_STOD035(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD035
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf035
WHERE count>0;
drop table taxish20150405_Stmax035;
drop table taxish20150405_Stmin035;
drop table taxish20150405_STODf035;
FROM taxish20150405_STOD035
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","035",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO035;
CREATE TABLE taxish20150405_STO035(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD035;
CREATE TABLE taxish20150405_STD035(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO035
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp035
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD035
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp035
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP035;
CREATE TABLE taxish20150405_STTP035(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO035 o,taxish20150405_STD035 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP035
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO035;
drop table taxish20150405_STD035;
drop table taxish20150405_STODp035;
DROP TABLE IF EXISTS taxish20150405_STTPn035;
CREATE TABLE taxish20150405_STTPn035(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP035 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn035
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg035;
CREATE TABLE taxish20150405_stagg035(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg035
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St035 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn035;
CREATE TABLE taxish20150405_staggn035(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn035
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St035 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1035;
CREATE TABLE taxish20150405_agg1035(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St035) bins
INSERT OVERWRITE TABLE taxish20150405_agg1035
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2035;
CREATE TABLE taxish20150405_agg2035(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St035) bins
INSERT OVERWRITE TABLE taxish20150405_agg2035
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1035
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","035",MIN(c),MAX(c);
FROM taxish20150405_agg2035
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","035",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time040;
CREATE TABLE taxish20150405_time040(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 04:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time040
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 04:30:00';

DROP TABLE IF EXISTS taxish20150405_St040;
CREATE EXTERNAL TABLE taxish20150405_St040(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St040
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time040 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time040;
DROP TABLE IF EXISTS taxish20150405_Stmax040;
CREATE TABLE taxish20150405_Stmax040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin040;
CREATE TABLE taxish20150405_Stmin040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St040 t GROUP BY t.carId) ts,taxish20150405_St040 t
INSERT OVERWRITE TABLE taxish20150405_Stmax040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St040 t GROUP BY t.carId) ts,taxish20150405_St040 t
INSERT OVERWRITE TABLE taxish20150405_Stmin040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp040;
CREATE TABLE taxish20150405_STODp040(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin040 tmin,taxish20150405_Stmax040 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp040
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf040;
CREATE TABLE taxish20150405_STODf040(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf040
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp040 od1 JOIN taxish20150405_STODp040 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD040;
CREATE TABLE taxish20150405_STOD040(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD040
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf040
WHERE count>0;
drop table taxish20150405_Stmax040;
drop table taxish20150405_Stmin040;
drop table taxish20150405_STODf040;
FROM taxish20150405_STOD040
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","040",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO040;
CREATE TABLE taxish20150405_STO040(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD040;
CREATE TABLE taxish20150405_STD040(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO040
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp040
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD040
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp040
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP040;
CREATE TABLE taxish20150405_STTP040(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO040 o,taxish20150405_STD040 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP040
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO040;
drop table taxish20150405_STD040;
drop table taxish20150405_STODp040;
DROP TABLE IF EXISTS taxish20150405_STTPn040;
CREATE TABLE taxish20150405_STTPn040(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP040 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn040
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg040;
CREATE TABLE taxish20150405_stagg040(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg040
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St040 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn040;
CREATE TABLE taxish20150405_staggn040(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn040
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St040 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1040;
CREATE TABLE taxish20150405_agg1040(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St040) bins
INSERT OVERWRITE TABLE taxish20150405_agg1040
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2040;
CREATE TABLE taxish20150405_agg2040(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St040) bins
INSERT OVERWRITE TABLE taxish20150405_agg2040
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1040
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","040",MIN(c),MAX(c);
FROM taxish20150405_agg2040
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","040",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time045;
CREATE TABLE taxish20150405_time045(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 04:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time045
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 05:00:00';

DROP TABLE IF EXISTS taxish20150405_St045;
CREATE EXTERNAL TABLE taxish20150405_St045(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St045
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time045 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time045;
DROP TABLE IF EXISTS taxish20150405_Stmax045;
CREATE TABLE taxish20150405_Stmax045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin045;
CREATE TABLE taxish20150405_Stmin045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St045 t GROUP BY t.carId) ts,taxish20150405_St045 t
INSERT OVERWRITE TABLE taxish20150405_Stmax045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St045 t GROUP BY t.carId) ts,taxish20150405_St045 t
INSERT OVERWRITE TABLE taxish20150405_Stmin045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp045;
CREATE TABLE taxish20150405_STODp045(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin045 tmin,taxish20150405_Stmax045 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp045
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf045;
CREATE TABLE taxish20150405_STODf045(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf045
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp045 od1 JOIN taxish20150405_STODp045 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD045;
CREATE TABLE taxish20150405_STOD045(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD045
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf045
WHERE count>0;
drop table taxish20150405_Stmax045;
drop table taxish20150405_Stmin045;
drop table taxish20150405_STODf045;
FROM taxish20150405_STOD045
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","045",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO045;
CREATE TABLE taxish20150405_STO045(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD045;
CREATE TABLE taxish20150405_STD045(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO045
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp045
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD045
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp045
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP045;
CREATE TABLE taxish20150405_STTP045(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO045 o,taxish20150405_STD045 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP045
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO045;
drop table taxish20150405_STD045;
drop table taxish20150405_STODp045;
DROP TABLE IF EXISTS taxish20150405_STTPn045;
CREATE TABLE taxish20150405_STTPn045(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP045 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn045
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg045;
CREATE TABLE taxish20150405_stagg045(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg045
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St045 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn045;
CREATE TABLE taxish20150405_staggn045(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn045
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St045 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1045;
CREATE TABLE taxish20150405_agg1045(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St045) bins
INSERT OVERWRITE TABLE taxish20150405_agg1045
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2045;
CREATE TABLE taxish20150405_agg2045(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St045) bins
INSERT OVERWRITE TABLE taxish20150405_agg2045
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1045
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","045",MIN(c),MAX(c);
FROM taxish20150405_agg2045
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","045",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time050;
CREATE TABLE taxish20150405_time050(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 05:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time050
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 05:30:00';

DROP TABLE IF EXISTS taxish20150405_St050;
CREATE EXTERNAL TABLE taxish20150405_St050(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St050
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time050 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time050;
DROP TABLE IF EXISTS taxish20150405_Stmax050;
CREATE TABLE taxish20150405_Stmax050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin050;
CREATE TABLE taxish20150405_Stmin050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St050 t GROUP BY t.carId) ts,taxish20150405_St050 t
INSERT OVERWRITE TABLE taxish20150405_Stmax050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St050 t GROUP BY t.carId) ts,taxish20150405_St050 t
INSERT OVERWRITE TABLE taxish20150405_Stmin050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp050;
CREATE TABLE taxish20150405_STODp050(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin050 tmin,taxish20150405_Stmax050 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp050
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf050;
CREATE TABLE taxish20150405_STODf050(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf050
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp050 od1 JOIN taxish20150405_STODp050 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD050;
CREATE TABLE taxish20150405_STOD050(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD050
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf050
WHERE count>0;
drop table taxish20150405_Stmax050;
drop table taxish20150405_Stmin050;
drop table taxish20150405_STODf050;
FROM taxish20150405_STOD050
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","050",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO050;
CREATE TABLE taxish20150405_STO050(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD050;
CREATE TABLE taxish20150405_STD050(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO050
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp050
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD050
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp050
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP050;
CREATE TABLE taxish20150405_STTP050(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO050 o,taxish20150405_STD050 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP050
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO050;
drop table taxish20150405_STD050;
drop table taxish20150405_STODp050;
DROP TABLE IF EXISTS taxish20150405_STTPn050;
CREATE TABLE taxish20150405_STTPn050(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP050 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn050
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg050;
CREATE TABLE taxish20150405_stagg050(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg050
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St050 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn050;
CREATE TABLE taxish20150405_staggn050(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn050
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St050 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1050;
CREATE TABLE taxish20150405_agg1050(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St050) bins
INSERT OVERWRITE TABLE taxish20150405_agg1050
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2050;
CREATE TABLE taxish20150405_agg2050(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St050) bins
INSERT OVERWRITE TABLE taxish20150405_agg2050
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1050
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","050",MIN(c),MAX(c);
FROM taxish20150405_agg2050
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","050",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time055;
CREATE TABLE taxish20150405_time055(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 05:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time055
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 06:00:00';

DROP TABLE IF EXISTS taxish20150405_St055;
CREATE EXTERNAL TABLE taxish20150405_St055(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St055
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time055 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time055;
DROP TABLE IF EXISTS taxish20150405_Stmax055;
CREATE TABLE taxish20150405_Stmax055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin055;
CREATE TABLE taxish20150405_Stmin055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St055 t GROUP BY t.carId) ts,taxish20150405_St055 t
INSERT OVERWRITE TABLE taxish20150405_Stmax055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St055 t GROUP BY t.carId) ts,taxish20150405_St055 t
INSERT OVERWRITE TABLE taxish20150405_Stmin055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp055;
CREATE TABLE taxish20150405_STODp055(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin055 tmin,taxish20150405_Stmax055 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp055
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf055;
CREATE TABLE taxish20150405_STODf055(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf055
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp055 od1 JOIN taxish20150405_STODp055 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD055;
CREATE TABLE taxish20150405_STOD055(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD055
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf055
WHERE count>0;
drop table taxish20150405_Stmax055;
drop table taxish20150405_Stmin055;
drop table taxish20150405_STODf055;
FROM taxish20150405_STOD055
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","055",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO055;
CREATE TABLE taxish20150405_STO055(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD055;
CREATE TABLE taxish20150405_STD055(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO055
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp055
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD055
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp055
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP055;
CREATE TABLE taxish20150405_STTP055(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO055 o,taxish20150405_STD055 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP055
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO055;
drop table taxish20150405_STD055;
drop table taxish20150405_STODp055;
DROP TABLE IF EXISTS taxish20150405_STTPn055;
CREATE TABLE taxish20150405_STTPn055(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP055 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn055
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg055;
CREATE TABLE taxish20150405_stagg055(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg055
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St055 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn055;
CREATE TABLE taxish20150405_staggn055(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn055
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St055 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1055;
CREATE TABLE taxish20150405_agg1055(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St055) bins
INSERT OVERWRITE TABLE taxish20150405_agg1055
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2055;
CREATE TABLE taxish20150405_agg2055(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St055) bins
INSERT OVERWRITE TABLE taxish20150405_agg2055
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1055
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","055",MIN(c),MAX(c);
FROM taxish20150405_agg2055
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","055",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time060;
CREATE TABLE taxish20150405_time060(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 06:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time060
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 06:30:00';

DROP TABLE IF EXISTS taxish20150405_St060;
CREATE EXTERNAL TABLE taxish20150405_St060(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St060
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time060 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time060;
DROP TABLE IF EXISTS taxish20150405_Stmax060;
CREATE TABLE taxish20150405_Stmax060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin060;
CREATE TABLE taxish20150405_Stmin060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St060 t GROUP BY t.carId) ts,taxish20150405_St060 t
INSERT OVERWRITE TABLE taxish20150405_Stmax060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St060 t GROUP BY t.carId) ts,taxish20150405_St060 t
INSERT OVERWRITE TABLE taxish20150405_Stmin060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp060;
CREATE TABLE taxish20150405_STODp060(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin060 tmin,taxish20150405_Stmax060 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp060
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf060;
CREATE TABLE taxish20150405_STODf060(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf060
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp060 od1 JOIN taxish20150405_STODp060 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD060;
CREATE TABLE taxish20150405_STOD060(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD060
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf060
WHERE count>0;
drop table taxish20150405_Stmax060;
drop table taxish20150405_Stmin060;
drop table taxish20150405_STODf060;
FROM taxish20150405_STOD060
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","060",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO060;
CREATE TABLE taxish20150405_STO060(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD060;
CREATE TABLE taxish20150405_STD060(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO060
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp060
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD060
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp060
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP060;
CREATE TABLE taxish20150405_STTP060(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO060 o,taxish20150405_STD060 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP060
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO060;
drop table taxish20150405_STD060;
drop table taxish20150405_STODp060;
DROP TABLE IF EXISTS taxish20150405_STTPn060;
CREATE TABLE taxish20150405_STTPn060(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP060 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn060
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg060;
CREATE TABLE taxish20150405_stagg060(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg060
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St060 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn060;
CREATE TABLE taxish20150405_staggn060(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn060
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St060 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1060;
CREATE TABLE taxish20150405_agg1060(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St060) bins
INSERT OVERWRITE TABLE taxish20150405_agg1060
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2060;
CREATE TABLE taxish20150405_agg2060(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St060) bins
INSERT OVERWRITE TABLE taxish20150405_agg2060
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1060
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","060",MIN(c),MAX(c);
FROM taxish20150405_agg2060
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","060",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time065;
CREATE TABLE taxish20150405_time065(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 06:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time065
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 07:00:00';

DROP TABLE IF EXISTS taxish20150405_St065;
CREATE EXTERNAL TABLE taxish20150405_St065(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St065
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time065 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time065;
DROP TABLE IF EXISTS taxish20150405_Stmax065;
CREATE TABLE taxish20150405_Stmax065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin065;
CREATE TABLE taxish20150405_Stmin065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St065 t GROUP BY t.carId) ts,taxish20150405_St065 t
INSERT OVERWRITE TABLE taxish20150405_Stmax065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St065 t GROUP BY t.carId) ts,taxish20150405_St065 t
INSERT OVERWRITE TABLE taxish20150405_Stmin065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp065;
CREATE TABLE taxish20150405_STODp065(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin065 tmin,taxish20150405_Stmax065 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp065
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf065;
CREATE TABLE taxish20150405_STODf065(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf065
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp065 od1 JOIN taxish20150405_STODp065 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD065;
CREATE TABLE taxish20150405_STOD065(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD065
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf065
WHERE count>0;
drop table taxish20150405_Stmax065;
drop table taxish20150405_Stmin065;
drop table taxish20150405_STODf065;
FROM taxish20150405_STOD065
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","065",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO065;
CREATE TABLE taxish20150405_STO065(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD065;
CREATE TABLE taxish20150405_STD065(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO065
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp065
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD065
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp065
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP065;
CREATE TABLE taxish20150405_STTP065(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO065 o,taxish20150405_STD065 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP065
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO065;
drop table taxish20150405_STD065;
drop table taxish20150405_STODp065;
DROP TABLE IF EXISTS taxish20150405_STTPn065;
CREATE TABLE taxish20150405_STTPn065(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP065 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn065
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg065;
CREATE TABLE taxish20150405_stagg065(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg065
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St065 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn065;
CREATE TABLE taxish20150405_staggn065(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn065
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St065 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1065;
CREATE TABLE taxish20150405_agg1065(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St065) bins
INSERT OVERWRITE TABLE taxish20150405_agg1065
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2065;
CREATE TABLE taxish20150405_agg2065(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St065) bins
INSERT OVERWRITE TABLE taxish20150405_agg2065
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1065
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","065",MIN(c),MAX(c);
FROM taxish20150405_agg2065
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","065",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time070;
CREATE TABLE taxish20150405_time070(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 07:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time070
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 07:30:00';

DROP TABLE IF EXISTS taxish20150405_St070;
CREATE EXTERNAL TABLE taxish20150405_St070(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St070
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time070 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time070;
DROP TABLE IF EXISTS taxish20150405_Stmax070;
CREATE TABLE taxish20150405_Stmax070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin070;
CREATE TABLE taxish20150405_Stmin070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St070 t GROUP BY t.carId) ts,taxish20150405_St070 t
INSERT OVERWRITE TABLE taxish20150405_Stmax070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St070 t GROUP BY t.carId) ts,taxish20150405_St070 t
INSERT OVERWRITE TABLE taxish20150405_Stmin070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp070;
CREATE TABLE taxish20150405_STODp070(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin070 tmin,taxish20150405_Stmax070 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp070
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf070;
CREATE TABLE taxish20150405_STODf070(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf070
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp070 od1 JOIN taxish20150405_STODp070 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD070;
CREATE TABLE taxish20150405_STOD070(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD070
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf070
WHERE count>0;
drop table taxish20150405_Stmax070;
drop table taxish20150405_Stmin070;
drop table taxish20150405_STODf070;
FROM taxish20150405_STOD070
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","070",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO070;
CREATE TABLE taxish20150405_STO070(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD070;
CREATE TABLE taxish20150405_STD070(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO070
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp070
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD070
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp070
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP070;
CREATE TABLE taxish20150405_STTP070(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO070 o,taxish20150405_STD070 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP070
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO070;
drop table taxish20150405_STD070;
drop table taxish20150405_STODp070;
DROP TABLE IF EXISTS taxish20150405_STTPn070;
CREATE TABLE taxish20150405_STTPn070(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP070 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn070
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg070;
CREATE TABLE taxish20150405_stagg070(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg070
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St070 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn070;
CREATE TABLE taxish20150405_staggn070(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn070
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St070 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1070;
CREATE TABLE taxish20150405_agg1070(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St070) bins
INSERT OVERWRITE TABLE taxish20150405_agg1070
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2070;
CREATE TABLE taxish20150405_agg2070(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St070) bins
INSERT OVERWRITE TABLE taxish20150405_agg2070
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1070
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","070",MIN(c),MAX(c);
FROM taxish20150405_agg2070
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","070",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time075;
CREATE TABLE taxish20150405_time075(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 07:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time075
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 08:00:00';

DROP TABLE IF EXISTS taxish20150405_St075;
CREATE EXTERNAL TABLE taxish20150405_St075(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St075
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time075 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time075;
DROP TABLE IF EXISTS taxish20150405_Stmax075;
CREATE TABLE taxish20150405_Stmax075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin075;
CREATE TABLE taxish20150405_Stmin075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St075 t GROUP BY t.carId) ts,taxish20150405_St075 t
INSERT OVERWRITE TABLE taxish20150405_Stmax075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St075 t GROUP BY t.carId) ts,taxish20150405_St075 t
INSERT OVERWRITE TABLE taxish20150405_Stmin075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp075;
CREATE TABLE taxish20150405_STODp075(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin075 tmin,taxish20150405_Stmax075 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp075
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf075;
CREATE TABLE taxish20150405_STODf075(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf075
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp075 od1 JOIN taxish20150405_STODp075 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD075;
CREATE TABLE taxish20150405_STOD075(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD075
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf075
WHERE count>0;
drop table taxish20150405_Stmax075;
drop table taxish20150405_Stmin075;
drop table taxish20150405_STODf075;
FROM taxish20150405_STOD075
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","075",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO075;
CREATE TABLE taxish20150405_STO075(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD075;
CREATE TABLE taxish20150405_STD075(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO075
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp075
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD075
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp075
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP075;
CREATE TABLE taxish20150405_STTP075(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO075 o,taxish20150405_STD075 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP075
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO075;
drop table taxish20150405_STD075;
drop table taxish20150405_STODp075;
DROP TABLE IF EXISTS taxish20150405_STTPn075;
CREATE TABLE taxish20150405_STTPn075(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP075 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn075
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg075;
CREATE TABLE taxish20150405_stagg075(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg075
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St075 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn075;
CREATE TABLE taxish20150405_staggn075(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn075
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St075 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1075;
CREATE TABLE taxish20150405_agg1075(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St075) bins
INSERT OVERWRITE TABLE taxish20150405_agg1075
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2075;
CREATE TABLE taxish20150405_agg2075(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St075) bins
INSERT OVERWRITE TABLE taxish20150405_agg2075
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1075
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","075",MIN(c),MAX(c);
FROM taxish20150405_agg2075
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","075",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time080;
CREATE TABLE taxish20150405_time080(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 08:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time080
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 08:30:00';

DROP TABLE IF EXISTS taxish20150405_St080;
CREATE EXTERNAL TABLE taxish20150405_St080(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St080
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time080 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time080;
DROP TABLE IF EXISTS taxish20150405_Stmax080;
CREATE TABLE taxish20150405_Stmax080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin080;
CREATE TABLE taxish20150405_Stmin080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St080 t GROUP BY t.carId) ts,taxish20150405_St080 t
INSERT OVERWRITE TABLE taxish20150405_Stmax080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St080 t GROUP BY t.carId) ts,taxish20150405_St080 t
INSERT OVERWRITE TABLE taxish20150405_Stmin080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp080;
CREATE TABLE taxish20150405_STODp080(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin080 tmin,taxish20150405_Stmax080 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp080
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf080;
CREATE TABLE taxish20150405_STODf080(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf080
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp080 od1 JOIN taxish20150405_STODp080 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD080;
CREATE TABLE taxish20150405_STOD080(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD080
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf080
WHERE count>0;
drop table taxish20150405_Stmax080;
drop table taxish20150405_Stmin080;
drop table taxish20150405_STODf080;
FROM taxish20150405_STOD080
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","080",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO080;
CREATE TABLE taxish20150405_STO080(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD080;
CREATE TABLE taxish20150405_STD080(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO080
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp080
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD080
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp080
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP080;
CREATE TABLE taxish20150405_STTP080(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO080 o,taxish20150405_STD080 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP080
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO080;
drop table taxish20150405_STD080;
drop table taxish20150405_STODp080;
DROP TABLE IF EXISTS taxish20150405_STTPn080;
CREATE TABLE taxish20150405_STTPn080(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP080 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn080
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg080;
CREATE TABLE taxish20150405_stagg080(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg080
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St080 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn080;
CREATE TABLE taxish20150405_staggn080(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn080
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St080 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1080;
CREATE TABLE taxish20150405_agg1080(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St080) bins
INSERT OVERWRITE TABLE taxish20150405_agg1080
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2080;
CREATE TABLE taxish20150405_agg2080(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St080) bins
INSERT OVERWRITE TABLE taxish20150405_agg2080
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1080
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","080",MIN(c),MAX(c);
FROM taxish20150405_agg2080
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","080",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time085;
CREATE TABLE taxish20150405_time085(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 08:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time085
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 09:00:00';

DROP TABLE IF EXISTS taxish20150405_St085;
CREATE EXTERNAL TABLE taxish20150405_St085(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St085
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time085 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time085;
DROP TABLE IF EXISTS taxish20150405_Stmax085;
CREATE TABLE taxish20150405_Stmax085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin085;
CREATE TABLE taxish20150405_Stmin085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St085 t GROUP BY t.carId) ts,taxish20150405_St085 t
INSERT OVERWRITE TABLE taxish20150405_Stmax085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St085 t GROUP BY t.carId) ts,taxish20150405_St085 t
INSERT OVERWRITE TABLE taxish20150405_Stmin085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp085;
CREATE TABLE taxish20150405_STODp085(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin085 tmin,taxish20150405_Stmax085 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp085
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf085;
CREATE TABLE taxish20150405_STODf085(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf085
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp085 od1 JOIN taxish20150405_STODp085 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD085;
CREATE TABLE taxish20150405_STOD085(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD085
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf085
WHERE count>0;
drop table taxish20150405_Stmax085;
drop table taxish20150405_Stmin085;
drop table taxish20150405_STODf085;
FROM taxish20150405_STOD085
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","085",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO085;
CREATE TABLE taxish20150405_STO085(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD085;
CREATE TABLE taxish20150405_STD085(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO085
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp085
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD085
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp085
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP085;
CREATE TABLE taxish20150405_STTP085(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO085 o,taxish20150405_STD085 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP085
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO085;
drop table taxish20150405_STD085;
drop table taxish20150405_STODp085;
DROP TABLE IF EXISTS taxish20150405_STTPn085;
CREATE TABLE taxish20150405_STTPn085(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP085 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn085
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg085;
CREATE TABLE taxish20150405_stagg085(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg085
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St085 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn085;
CREATE TABLE taxish20150405_staggn085(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn085
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St085 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1085;
CREATE TABLE taxish20150405_agg1085(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St085) bins
INSERT OVERWRITE TABLE taxish20150405_agg1085
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2085;
CREATE TABLE taxish20150405_agg2085(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St085) bins
INSERT OVERWRITE TABLE taxish20150405_agg2085
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1085
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","085",MIN(c),MAX(c);
FROM taxish20150405_agg2085
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","085",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time090;
CREATE TABLE taxish20150405_time090(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 09:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time090
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 09:30:00';

DROP TABLE IF EXISTS taxish20150405_St090;
CREATE EXTERNAL TABLE taxish20150405_St090(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St090
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time090 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time090;
DROP TABLE IF EXISTS taxish20150405_Stmax090;
CREATE TABLE taxish20150405_Stmax090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin090;
CREATE TABLE taxish20150405_Stmin090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St090 t GROUP BY t.carId) ts,taxish20150405_St090 t
INSERT OVERWRITE TABLE taxish20150405_Stmax090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St090 t GROUP BY t.carId) ts,taxish20150405_St090 t
INSERT OVERWRITE TABLE taxish20150405_Stmin090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp090;
CREATE TABLE taxish20150405_STODp090(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin090 tmin,taxish20150405_Stmax090 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp090
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf090;
CREATE TABLE taxish20150405_STODf090(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf090
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp090 od1 JOIN taxish20150405_STODp090 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD090;
CREATE TABLE taxish20150405_STOD090(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD090
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf090
WHERE count>0;
drop table taxish20150405_Stmax090;
drop table taxish20150405_Stmin090;
drop table taxish20150405_STODf090;
FROM taxish20150405_STOD090
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","090",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO090;
CREATE TABLE taxish20150405_STO090(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD090;
CREATE TABLE taxish20150405_STD090(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO090
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp090
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD090
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp090
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP090;
CREATE TABLE taxish20150405_STTP090(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO090 o,taxish20150405_STD090 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP090
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO090;
drop table taxish20150405_STD090;
drop table taxish20150405_STODp090;
DROP TABLE IF EXISTS taxish20150405_STTPn090;
CREATE TABLE taxish20150405_STTPn090(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP090 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn090
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg090;
CREATE TABLE taxish20150405_stagg090(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg090
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St090 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn090;
CREATE TABLE taxish20150405_staggn090(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn090
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St090 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1090;
CREATE TABLE taxish20150405_agg1090(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St090) bins
INSERT OVERWRITE TABLE taxish20150405_agg1090
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2090;
CREATE TABLE taxish20150405_agg2090(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St090) bins
INSERT OVERWRITE TABLE taxish20150405_agg2090
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1090
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","090",MIN(c),MAX(c);
FROM taxish20150405_agg2090
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","090",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time095;
CREATE TABLE taxish20150405_time095(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 09:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time095
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 10:00:00';

DROP TABLE IF EXISTS taxish20150405_St095;
CREATE EXTERNAL TABLE taxish20150405_St095(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St095
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time095 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time095;
DROP TABLE IF EXISTS taxish20150405_Stmax095;
CREATE TABLE taxish20150405_Stmax095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin095;
CREATE TABLE taxish20150405_Stmin095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St095 t GROUP BY t.carId) ts,taxish20150405_St095 t
INSERT OVERWRITE TABLE taxish20150405_Stmax095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St095 t GROUP BY t.carId) ts,taxish20150405_St095 t
INSERT OVERWRITE TABLE taxish20150405_Stmin095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp095;
CREATE TABLE taxish20150405_STODp095(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin095 tmin,taxish20150405_Stmax095 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp095
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf095;
CREATE TABLE taxish20150405_STODf095(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf095
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp095 od1 JOIN taxish20150405_STODp095 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD095;
CREATE TABLE taxish20150405_STOD095(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD095
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf095
WHERE count>0;
drop table taxish20150405_Stmax095;
drop table taxish20150405_Stmin095;
drop table taxish20150405_STODf095;
FROM taxish20150405_STOD095
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","095",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO095;
CREATE TABLE taxish20150405_STO095(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD095;
CREATE TABLE taxish20150405_STD095(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO095
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp095
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD095
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp095
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP095;
CREATE TABLE taxish20150405_STTP095(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO095 o,taxish20150405_STD095 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP095
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO095;
drop table taxish20150405_STD095;
drop table taxish20150405_STODp095;
DROP TABLE IF EXISTS taxish20150405_STTPn095;
CREATE TABLE taxish20150405_STTPn095(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP095 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn095
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg095;
CREATE TABLE taxish20150405_stagg095(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg095
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St095 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn095;
CREATE TABLE taxish20150405_staggn095(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn095
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St095 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1095;
CREATE TABLE taxish20150405_agg1095(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St095) bins
INSERT OVERWRITE TABLE taxish20150405_agg1095
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2095;
CREATE TABLE taxish20150405_agg2095(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St095) bins
INSERT OVERWRITE TABLE taxish20150405_agg2095
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1095
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","095",MIN(c),MAX(c);
FROM taxish20150405_agg2095
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","095",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time100;
CREATE TABLE taxish20150405_time100(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 10:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time100
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 10:30:00';

DROP TABLE IF EXISTS taxish20150405_St100;
CREATE EXTERNAL TABLE taxish20150405_St100(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St100
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time100 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time100;
DROP TABLE IF EXISTS taxish20150405_Stmax100;
CREATE TABLE taxish20150405_Stmax100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin100;
CREATE TABLE taxish20150405_Stmin100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St100 t GROUP BY t.carId) ts,taxish20150405_St100 t
INSERT OVERWRITE TABLE taxish20150405_Stmax100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St100 t GROUP BY t.carId) ts,taxish20150405_St100 t
INSERT OVERWRITE TABLE taxish20150405_Stmin100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp100;
CREATE TABLE taxish20150405_STODp100(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin100 tmin,taxish20150405_Stmax100 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp100
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf100;
CREATE TABLE taxish20150405_STODf100(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf100
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp100 od1 JOIN taxish20150405_STODp100 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD100;
CREATE TABLE taxish20150405_STOD100(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD100
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf100
WHERE count>0;
drop table taxish20150405_Stmax100;
drop table taxish20150405_Stmin100;
drop table taxish20150405_STODf100;
FROM taxish20150405_STOD100
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","100",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO100;
CREATE TABLE taxish20150405_STO100(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD100;
CREATE TABLE taxish20150405_STD100(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO100
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp100
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD100
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp100
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP100;
CREATE TABLE taxish20150405_STTP100(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO100 o,taxish20150405_STD100 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP100
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO100;
drop table taxish20150405_STD100;
drop table taxish20150405_STODp100;
DROP TABLE IF EXISTS taxish20150405_STTPn100;
CREATE TABLE taxish20150405_STTPn100(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP100 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn100
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg100;
CREATE TABLE taxish20150405_stagg100(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg100
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St100 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn100;
CREATE TABLE taxish20150405_staggn100(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn100
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St100 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1100;
CREATE TABLE taxish20150405_agg1100(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St100) bins
INSERT OVERWRITE TABLE taxish20150405_agg1100
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2100;
CREATE TABLE taxish20150405_agg2100(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St100) bins
INSERT OVERWRITE TABLE taxish20150405_agg2100
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1100
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","100",MIN(c),MAX(c);
FROM taxish20150405_agg2100
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","100",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time105;
CREATE TABLE taxish20150405_time105(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 10:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time105
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 11:00:00';

DROP TABLE IF EXISTS taxish20150405_St105;
CREATE EXTERNAL TABLE taxish20150405_St105(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St105
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time105 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time105;
DROP TABLE IF EXISTS taxish20150405_Stmax105;
CREATE TABLE taxish20150405_Stmax105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin105;
CREATE TABLE taxish20150405_Stmin105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St105 t GROUP BY t.carId) ts,taxish20150405_St105 t
INSERT OVERWRITE TABLE taxish20150405_Stmax105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St105 t GROUP BY t.carId) ts,taxish20150405_St105 t
INSERT OVERWRITE TABLE taxish20150405_Stmin105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp105;
CREATE TABLE taxish20150405_STODp105(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin105 tmin,taxish20150405_Stmax105 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp105
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf105;
CREATE TABLE taxish20150405_STODf105(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf105
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp105 od1 JOIN taxish20150405_STODp105 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD105;
CREATE TABLE taxish20150405_STOD105(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD105
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf105
WHERE count>0;
drop table taxish20150405_Stmax105;
drop table taxish20150405_Stmin105;
drop table taxish20150405_STODf105;
FROM taxish20150405_STOD105
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","105",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO105;
CREATE TABLE taxish20150405_STO105(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD105;
CREATE TABLE taxish20150405_STD105(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO105
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp105
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD105
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp105
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP105;
CREATE TABLE taxish20150405_STTP105(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO105 o,taxish20150405_STD105 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP105
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO105;
drop table taxish20150405_STD105;
drop table taxish20150405_STODp105;
DROP TABLE IF EXISTS taxish20150405_STTPn105;
CREATE TABLE taxish20150405_STTPn105(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP105 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn105
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg105;
CREATE TABLE taxish20150405_stagg105(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg105
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St105 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn105;
CREATE TABLE taxish20150405_staggn105(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn105
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St105 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1105;
CREATE TABLE taxish20150405_agg1105(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St105) bins
INSERT OVERWRITE TABLE taxish20150405_agg1105
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2105;
CREATE TABLE taxish20150405_agg2105(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St105) bins
INSERT OVERWRITE TABLE taxish20150405_agg2105
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1105
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","105",MIN(c),MAX(c);
FROM taxish20150405_agg2105
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","105",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time110;
CREATE TABLE taxish20150405_time110(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 11:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time110
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 11:30:00';

DROP TABLE IF EXISTS taxish20150405_St110;
CREATE EXTERNAL TABLE taxish20150405_St110(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St110
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time110 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time110;
DROP TABLE IF EXISTS taxish20150405_Stmax110;
CREATE TABLE taxish20150405_Stmax110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin110;
CREATE TABLE taxish20150405_Stmin110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St110 t GROUP BY t.carId) ts,taxish20150405_St110 t
INSERT OVERWRITE TABLE taxish20150405_Stmax110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St110 t GROUP BY t.carId) ts,taxish20150405_St110 t
INSERT OVERWRITE TABLE taxish20150405_Stmin110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp110;
CREATE TABLE taxish20150405_STODp110(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin110 tmin,taxish20150405_Stmax110 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp110
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf110;
CREATE TABLE taxish20150405_STODf110(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf110
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp110 od1 JOIN taxish20150405_STODp110 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD110;
CREATE TABLE taxish20150405_STOD110(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD110
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf110
WHERE count>0;
drop table taxish20150405_Stmax110;
drop table taxish20150405_Stmin110;
drop table taxish20150405_STODf110;
FROM taxish20150405_STOD110
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","110",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO110;
CREATE TABLE taxish20150405_STO110(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD110;
CREATE TABLE taxish20150405_STD110(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO110
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp110
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD110
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp110
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP110;
CREATE TABLE taxish20150405_STTP110(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO110 o,taxish20150405_STD110 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP110
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO110;
drop table taxish20150405_STD110;
drop table taxish20150405_STODp110;
DROP TABLE IF EXISTS taxish20150405_STTPn110;
CREATE TABLE taxish20150405_STTPn110(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP110 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn110
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg110;
CREATE TABLE taxish20150405_stagg110(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg110
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St110 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn110;
CREATE TABLE taxish20150405_staggn110(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn110
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St110 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1110;
CREATE TABLE taxish20150405_agg1110(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St110) bins
INSERT OVERWRITE TABLE taxish20150405_agg1110
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2110;
CREATE TABLE taxish20150405_agg2110(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St110) bins
INSERT OVERWRITE TABLE taxish20150405_agg2110
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1110
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","110",MIN(c),MAX(c);
FROM taxish20150405_agg2110
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","110",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time115;
CREATE TABLE taxish20150405_time115(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 11:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time115
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 12:00:00';

DROP TABLE IF EXISTS taxish20150405_St115;
CREATE EXTERNAL TABLE taxish20150405_St115(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St115
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time115 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time115;
DROP TABLE IF EXISTS taxish20150405_Stmax115;
CREATE TABLE taxish20150405_Stmax115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin115;
CREATE TABLE taxish20150405_Stmin115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St115 t GROUP BY t.carId) ts,taxish20150405_St115 t
INSERT OVERWRITE TABLE taxish20150405_Stmax115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St115 t GROUP BY t.carId) ts,taxish20150405_St115 t
INSERT OVERWRITE TABLE taxish20150405_Stmin115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp115;
CREATE TABLE taxish20150405_STODp115(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin115 tmin,taxish20150405_Stmax115 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp115
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf115;
CREATE TABLE taxish20150405_STODf115(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf115
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp115 od1 JOIN taxish20150405_STODp115 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD115;
CREATE TABLE taxish20150405_STOD115(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD115
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf115
WHERE count>0;
drop table taxish20150405_Stmax115;
drop table taxish20150405_Stmin115;
drop table taxish20150405_STODf115;
FROM taxish20150405_STOD115
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","115",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO115;
CREATE TABLE taxish20150405_STO115(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD115;
CREATE TABLE taxish20150405_STD115(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO115
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp115
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD115
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp115
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP115;
CREATE TABLE taxish20150405_STTP115(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO115 o,taxish20150405_STD115 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP115
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO115;
drop table taxish20150405_STD115;
drop table taxish20150405_STODp115;
DROP TABLE IF EXISTS taxish20150405_STTPn115;
CREATE TABLE taxish20150405_STTPn115(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP115 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn115
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg115;
CREATE TABLE taxish20150405_stagg115(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg115
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St115 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn115;
CREATE TABLE taxish20150405_staggn115(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn115
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St115 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1115;
CREATE TABLE taxish20150405_agg1115(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St115) bins
INSERT OVERWRITE TABLE taxish20150405_agg1115
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2115;
CREATE TABLE taxish20150405_agg2115(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St115) bins
INSERT OVERWRITE TABLE taxish20150405_agg2115
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1115
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","115",MIN(c),MAX(c);
FROM taxish20150405_agg2115
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","115",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time120;
CREATE TABLE taxish20150405_time120(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 12:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time120
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 12:30:00';

DROP TABLE IF EXISTS taxish20150405_St120;
CREATE EXTERNAL TABLE taxish20150405_St120(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St120
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time120 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time120;
DROP TABLE IF EXISTS taxish20150405_Stmax120;
CREATE TABLE taxish20150405_Stmax120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin120;
CREATE TABLE taxish20150405_Stmin120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St120 t GROUP BY t.carId) ts,taxish20150405_St120 t
INSERT OVERWRITE TABLE taxish20150405_Stmax120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St120 t GROUP BY t.carId) ts,taxish20150405_St120 t
INSERT OVERWRITE TABLE taxish20150405_Stmin120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp120;
CREATE TABLE taxish20150405_STODp120(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin120 tmin,taxish20150405_Stmax120 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp120
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf120;
CREATE TABLE taxish20150405_STODf120(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf120
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp120 od1 JOIN taxish20150405_STODp120 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD120;
CREATE TABLE taxish20150405_STOD120(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD120
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf120
WHERE count>0;
drop table taxish20150405_Stmax120;
drop table taxish20150405_Stmin120;
drop table taxish20150405_STODf120;
FROM taxish20150405_STOD120
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","120",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO120;
CREATE TABLE taxish20150405_STO120(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD120;
CREATE TABLE taxish20150405_STD120(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO120
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp120
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD120
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp120
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP120;
CREATE TABLE taxish20150405_STTP120(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO120 o,taxish20150405_STD120 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP120
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO120;
drop table taxish20150405_STD120;
drop table taxish20150405_STODp120;
DROP TABLE IF EXISTS taxish20150405_STTPn120;
CREATE TABLE taxish20150405_STTPn120(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP120 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn120
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg120;
CREATE TABLE taxish20150405_stagg120(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg120
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St120 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn120;
CREATE TABLE taxish20150405_staggn120(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn120
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St120 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1120;
CREATE TABLE taxish20150405_agg1120(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St120) bins
INSERT OVERWRITE TABLE taxish20150405_agg1120
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2120;
CREATE TABLE taxish20150405_agg2120(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St120) bins
INSERT OVERWRITE TABLE taxish20150405_agg2120
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1120
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","120",MIN(c),MAX(c);
FROM taxish20150405_agg2120
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","120",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time125;
CREATE TABLE taxish20150405_time125(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 12:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time125
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 13:00:00';

DROP TABLE IF EXISTS taxish20150405_St125;
CREATE EXTERNAL TABLE taxish20150405_St125(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St125
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time125 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time125;
DROP TABLE IF EXISTS taxish20150405_Stmax125;
CREATE TABLE taxish20150405_Stmax125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin125;
CREATE TABLE taxish20150405_Stmin125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St125 t GROUP BY t.carId) ts,taxish20150405_St125 t
INSERT OVERWRITE TABLE taxish20150405_Stmax125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St125 t GROUP BY t.carId) ts,taxish20150405_St125 t
INSERT OVERWRITE TABLE taxish20150405_Stmin125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp125;
CREATE TABLE taxish20150405_STODp125(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin125 tmin,taxish20150405_Stmax125 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp125
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf125;
CREATE TABLE taxish20150405_STODf125(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf125
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp125 od1 JOIN taxish20150405_STODp125 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD125;
CREATE TABLE taxish20150405_STOD125(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD125
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf125
WHERE count>0;
drop table taxish20150405_Stmax125;
drop table taxish20150405_Stmin125;
drop table taxish20150405_STODf125;
FROM taxish20150405_STOD125
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","125",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO125;
CREATE TABLE taxish20150405_STO125(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD125;
CREATE TABLE taxish20150405_STD125(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO125
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp125
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD125
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp125
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP125;
CREATE TABLE taxish20150405_STTP125(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO125 o,taxish20150405_STD125 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP125
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO125;
drop table taxish20150405_STD125;
drop table taxish20150405_STODp125;
DROP TABLE IF EXISTS taxish20150405_STTPn125;
CREATE TABLE taxish20150405_STTPn125(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP125 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn125
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg125;
CREATE TABLE taxish20150405_stagg125(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg125
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St125 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn125;
CREATE TABLE taxish20150405_staggn125(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn125
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St125 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1125;
CREATE TABLE taxish20150405_agg1125(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St125) bins
INSERT OVERWRITE TABLE taxish20150405_agg1125
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2125;
CREATE TABLE taxish20150405_agg2125(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St125) bins
INSERT OVERWRITE TABLE taxish20150405_agg2125
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1125
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","125",MIN(c),MAX(c);
FROM taxish20150405_agg2125
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","125",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time130;
CREATE TABLE taxish20150405_time130(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 13:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time130
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 13:30:00';

DROP TABLE IF EXISTS taxish20150405_St130;
CREATE EXTERNAL TABLE taxish20150405_St130(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St130
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time130 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time130;
DROP TABLE IF EXISTS taxish20150405_Stmax130;
CREATE TABLE taxish20150405_Stmax130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin130;
CREATE TABLE taxish20150405_Stmin130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St130 t GROUP BY t.carId) ts,taxish20150405_St130 t
INSERT OVERWRITE TABLE taxish20150405_Stmax130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St130 t GROUP BY t.carId) ts,taxish20150405_St130 t
INSERT OVERWRITE TABLE taxish20150405_Stmin130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp130;
CREATE TABLE taxish20150405_STODp130(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin130 tmin,taxish20150405_Stmax130 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp130
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf130;
CREATE TABLE taxish20150405_STODf130(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf130
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp130 od1 JOIN taxish20150405_STODp130 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD130;
CREATE TABLE taxish20150405_STOD130(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD130
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf130
WHERE count>0;
drop table taxish20150405_Stmax130;
drop table taxish20150405_Stmin130;
drop table taxish20150405_STODf130;
FROM taxish20150405_STOD130
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","130",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO130;
CREATE TABLE taxish20150405_STO130(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD130;
CREATE TABLE taxish20150405_STD130(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO130
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp130
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD130
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp130
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP130;
CREATE TABLE taxish20150405_STTP130(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO130 o,taxish20150405_STD130 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP130
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO130;
drop table taxish20150405_STD130;
drop table taxish20150405_STODp130;
DROP TABLE IF EXISTS taxish20150405_STTPn130;
CREATE TABLE taxish20150405_STTPn130(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP130 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn130
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg130;
CREATE TABLE taxish20150405_stagg130(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg130
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St130 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn130;
CREATE TABLE taxish20150405_staggn130(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn130
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St130 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1130;
CREATE TABLE taxish20150405_agg1130(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St130) bins
INSERT OVERWRITE TABLE taxish20150405_agg1130
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2130;
CREATE TABLE taxish20150405_agg2130(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St130) bins
INSERT OVERWRITE TABLE taxish20150405_agg2130
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1130
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","130",MIN(c),MAX(c);
FROM taxish20150405_agg2130
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","130",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time135;
CREATE TABLE taxish20150405_time135(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 13:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time135
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 14:00:00';

DROP TABLE IF EXISTS taxish20150405_St135;
CREATE EXTERNAL TABLE taxish20150405_St135(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St135
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time135 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time135;
DROP TABLE IF EXISTS taxish20150405_Stmax135;
CREATE TABLE taxish20150405_Stmax135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin135;
CREATE TABLE taxish20150405_Stmin135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St135 t GROUP BY t.carId) ts,taxish20150405_St135 t
INSERT OVERWRITE TABLE taxish20150405_Stmax135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St135 t GROUP BY t.carId) ts,taxish20150405_St135 t
INSERT OVERWRITE TABLE taxish20150405_Stmin135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp135;
CREATE TABLE taxish20150405_STODp135(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin135 tmin,taxish20150405_Stmax135 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp135
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf135;
CREATE TABLE taxish20150405_STODf135(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf135
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp135 od1 JOIN taxish20150405_STODp135 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD135;
CREATE TABLE taxish20150405_STOD135(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD135
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf135
WHERE count>0;
drop table taxish20150405_Stmax135;
drop table taxish20150405_Stmin135;
drop table taxish20150405_STODf135;
FROM taxish20150405_STOD135
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","135",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO135;
CREATE TABLE taxish20150405_STO135(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD135;
CREATE TABLE taxish20150405_STD135(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO135
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp135
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD135
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp135
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP135;
CREATE TABLE taxish20150405_STTP135(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO135 o,taxish20150405_STD135 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP135
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO135;
drop table taxish20150405_STD135;
drop table taxish20150405_STODp135;
DROP TABLE IF EXISTS taxish20150405_STTPn135;
CREATE TABLE taxish20150405_STTPn135(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP135 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn135
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg135;
CREATE TABLE taxish20150405_stagg135(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg135
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St135 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn135;
CREATE TABLE taxish20150405_staggn135(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn135
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St135 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1135;
CREATE TABLE taxish20150405_agg1135(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St135) bins
INSERT OVERWRITE TABLE taxish20150405_agg1135
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2135;
CREATE TABLE taxish20150405_agg2135(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St135) bins
INSERT OVERWRITE TABLE taxish20150405_agg2135
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1135
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","135",MIN(c),MAX(c);
FROM taxish20150405_agg2135
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","135",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time140;
CREATE TABLE taxish20150405_time140(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 14:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time140
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 14:30:00';

DROP TABLE IF EXISTS taxish20150405_St140;
CREATE EXTERNAL TABLE taxish20150405_St140(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St140
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time140 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time140;
DROP TABLE IF EXISTS taxish20150405_Stmax140;
CREATE TABLE taxish20150405_Stmax140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin140;
CREATE TABLE taxish20150405_Stmin140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St140 t GROUP BY t.carId) ts,taxish20150405_St140 t
INSERT OVERWRITE TABLE taxish20150405_Stmax140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St140 t GROUP BY t.carId) ts,taxish20150405_St140 t
INSERT OVERWRITE TABLE taxish20150405_Stmin140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp140;
CREATE TABLE taxish20150405_STODp140(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin140 tmin,taxish20150405_Stmax140 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp140
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf140;
CREATE TABLE taxish20150405_STODf140(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf140
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp140 od1 JOIN taxish20150405_STODp140 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD140;
CREATE TABLE taxish20150405_STOD140(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD140
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf140
WHERE count>0;
drop table taxish20150405_Stmax140;
drop table taxish20150405_Stmin140;
drop table taxish20150405_STODf140;
FROM taxish20150405_STOD140
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","140",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO140;
CREATE TABLE taxish20150405_STO140(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD140;
CREATE TABLE taxish20150405_STD140(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO140
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp140
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD140
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp140
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP140;
CREATE TABLE taxish20150405_STTP140(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO140 o,taxish20150405_STD140 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP140
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO140;
drop table taxish20150405_STD140;
drop table taxish20150405_STODp140;
DROP TABLE IF EXISTS taxish20150405_STTPn140;
CREATE TABLE taxish20150405_STTPn140(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP140 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn140
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg140;
CREATE TABLE taxish20150405_stagg140(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg140
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St140 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn140;
CREATE TABLE taxish20150405_staggn140(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn140
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St140 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1140;
CREATE TABLE taxish20150405_agg1140(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St140) bins
INSERT OVERWRITE TABLE taxish20150405_agg1140
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2140;
CREATE TABLE taxish20150405_agg2140(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St140) bins
INSERT OVERWRITE TABLE taxish20150405_agg2140
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1140
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","140",MIN(c),MAX(c);
FROM taxish20150405_agg2140
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","140",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time145;
CREATE TABLE taxish20150405_time145(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 14:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time145
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 15:00:00';

DROP TABLE IF EXISTS taxish20150405_St145;
CREATE EXTERNAL TABLE taxish20150405_St145(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St145
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time145 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time145;
DROP TABLE IF EXISTS taxish20150405_Stmax145;
CREATE TABLE taxish20150405_Stmax145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin145;
CREATE TABLE taxish20150405_Stmin145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St145 t GROUP BY t.carId) ts,taxish20150405_St145 t
INSERT OVERWRITE TABLE taxish20150405_Stmax145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St145 t GROUP BY t.carId) ts,taxish20150405_St145 t
INSERT OVERWRITE TABLE taxish20150405_Stmin145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp145;
CREATE TABLE taxish20150405_STODp145(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin145 tmin,taxish20150405_Stmax145 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp145
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf145;
CREATE TABLE taxish20150405_STODf145(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf145
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp145 od1 JOIN taxish20150405_STODp145 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD145;
CREATE TABLE taxish20150405_STOD145(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD145
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf145
WHERE count>0;
drop table taxish20150405_Stmax145;
drop table taxish20150405_Stmin145;
drop table taxish20150405_STODf145;
FROM taxish20150405_STOD145
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","145",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO145;
CREATE TABLE taxish20150405_STO145(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD145;
CREATE TABLE taxish20150405_STD145(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO145
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp145
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD145
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp145
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP145;
CREATE TABLE taxish20150405_STTP145(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO145 o,taxish20150405_STD145 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP145
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO145;
drop table taxish20150405_STD145;
drop table taxish20150405_STODp145;
DROP TABLE IF EXISTS taxish20150405_STTPn145;
CREATE TABLE taxish20150405_STTPn145(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP145 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn145
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg145;
CREATE TABLE taxish20150405_stagg145(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg145
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St145 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn145;
CREATE TABLE taxish20150405_staggn145(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn145
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St145 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1145;
CREATE TABLE taxish20150405_agg1145(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St145) bins
INSERT OVERWRITE TABLE taxish20150405_agg1145
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2145;
CREATE TABLE taxish20150405_agg2145(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St145) bins
INSERT OVERWRITE TABLE taxish20150405_agg2145
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1145
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","145",MIN(c),MAX(c);
FROM taxish20150405_agg2145
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","145",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time150;
CREATE TABLE taxish20150405_time150(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 15:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time150
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 15:30:00';

DROP TABLE IF EXISTS taxish20150405_St150;
CREATE EXTERNAL TABLE taxish20150405_St150(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St150
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time150 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time150;
DROP TABLE IF EXISTS taxish20150405_Stmax150;
CREATE TABLE taxish20150405_Stmax150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin150;
CREATE TABLE taxish20150405_Stmin150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St150 t GROUP BY t.carId) ts,taxish20150405_St150 t
INSERT OVERWRITE TABLE taxish20150405_Stmax150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St150 t GROUP BY t.carId) ts,taxish20150405_St150 t
INSERT OVERWRITE TABLE taxish20150405_Stmin150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp150;
CREATE TABLE taxish20150405_STODp150(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin150 tmin,taxish20150405_Stmax150 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp150
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf150;
CREATE TABLE taxish20150405_STODf150(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf150
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp150 od1 JOIN taxish20150405_STODp150 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD150;
CREATE TABLE taxish20150405_STOD150(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD150
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf150
WHERE count>0;
drop table taxish20150405_Stmax150;
drop table taxish20150405_Stmin150;
drop table taxish20150405_STODf150;
FROM taxish20150405_STOD150
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","150",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO150;
CREATE TABLE taxish20150405_STO150(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD150;
CREATE TABLE taxish20150405_STD150(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO150
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp150
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD150
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp150
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP150;
CREATE TABLE taxish20150405_STTP150(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO150 o,taxish20150405_STD150 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP150
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO150;
drop table taxish20150405_STD150;
drop table taxish20150405_STODp150;
DROP TABLE IF EXISTS taxish20150405_STTPn150;
CREATE TABLE taxish20150405_STTPn150(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP150 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn150
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg150;
CREATE TABLE taxish20150405_stagg150(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg150
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St150 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn150;
CREATE TABLE taxish20150405_staggn150(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn150
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St150 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1150;
CREATE TABLE taxish20150405_agg1150(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St150) bins
INSERT OVERWRITE TABLE taxish20150405_agg1150
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2150;
CREATE TABLE taxish20150405_agg2150(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St150) bins
INSERT OVERWRITE TABLE taxish20150405_agg2150
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1150
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","150",MIN(c),MAX(c);
FROM taxish20150405_agg2150
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","150",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time155;
CREATE TABLE taxish20150405_time155(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 15:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time155
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 16:00:00';

DROP TABLE IF EXISTS taxish20150405_St155;
CREATE EXTERNAL TABLE taxish20150405_St155(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St155
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time155 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time155;
DROP TABLE IF EXISTS taxish20150405_Stmax155;
CREATE TABLE taxish20150405_Stmax155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin155;
CREATE TABLE taxish20150405_Stmin155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St155 t GROUP BY t.carId) ts,taxish20150405_St155 t
INSERT OVERWRITE TABLE taxish20150405_Stmax155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St155 t GROUP BY t.carId) ts,taxish20150405_St155 t
INSERT OVERWRITE TABLE taxish20150405_Stmin155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp155;
CREATE TABLE taxish20150405_STODp155(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin155 tmin,taxish20150405_Stmax155 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp155
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf155;
CREATE TABLE taxish20150405_STODf155(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf155
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp155 od1 JOIN taxish20150405_STODp155 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD155;
CREATE TABLE taxish20150405_STOD155(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD155
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf155
WHERE count>0;
drop table taxish20150405_Stmax155;
drop table taxish20150405_Stmin155;
drop table taxish20150405_STODf155;
FROM taxish20150405_STOD155
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","155",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO155;
CREATE TABLE taxish20150405_STO155(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD155;
CREATE TABLE taxish20150405_STD155(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO155
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp155
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD155
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp155
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP155;
CREATE TABLE taxish20150405_STTP155(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO155 o,taxish20150405_STD155 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP155
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO155;
drop table taxish20150405_STD155;
drop table taxish20150405_STODp155;
DROP TABLE IF EXISTS taxish20150405_STTPn155;
CREATE TABLE taxish20150405_STTPn155(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP155 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn155
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg155;
CREATE TABLE taxish20150405_stagg155(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg155
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St155 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn155;
CREATE TABLE taxish20150405_staggn155(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn155
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St155 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1155;
CREATE TABLE taxish20150405_agg1155(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St155) bins
INSERT OVERWRITE TABLE taxish20150405_agg1155
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2155;
CREATE TABLE taxish20150405_agg2155(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St155) bins
INSERT OVERWRITE TABLE taxish20150405_agg2155
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1155
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","155",MIN(c),MAX(c);
FROM taxish20150405_agg2155
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","155",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time160;
CREATE TABLE taxish20150405_time160(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 16:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time160
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 16:30:00';

DROP TABLE IF EXISTS taxish20150405_St160;
CREATE EXTERNAL TABLE taxish20150405_St160(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St160
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time160 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time160;
DROP TABLE IF EXISTS taxish20150405_Stmax160;
CREATE TABLE taxish20150405_Stmax160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin160;
CREATE TABLE taxish20150405_Stmin160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St160 t GROUP BY t.carId) ts,taxish20150405_St160 t
INSERT OVERWRITE TABLE taxish20150405_Stmax160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St160 t GROUP BY t.carId) ts,taxish20150405_St160 t
INSERT OVERWRITE TABLE taxish20150405_Stmin160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp160;
CREATE TABLE taxish20150405_STODp160(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin160 tmin,taxish20150405_Stmax160 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp160
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf160;
CREATE TABLE taxish20150405_STODf160(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf160
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp160 od1 JOIN taxish20150405_STODp160 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD160;
CREATE TABLE taxish20150405_STOD160(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD160
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf160
WHERE count>0;
drop table taxish20150405_Stmax160;
drop table taxish20150405_Stmin160;
drop table taxish20150405_STODf160;
FROM taxish20150405_STOD160
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","160",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO160;
CREATE TABLE taxish20150405_STO160(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD160;
CREATE TABLE taxish20150405_STD160(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO160
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp160
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD160
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp160
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP160;
CREATE TABLE taxish20150405_STTP160(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO160 o,taxish20150405_STD160 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP160
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO160;
drop table taxish20150405_STD160;
drop table taxish20150405_STODp160;
DROP TABLE IF EXISTS taxish20150405_STTPn160;
CREATE TABLE taxish20150405_STTPn160(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP160 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn160
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg160;
CREATE TABLE taxish20150405_stagg160(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg160
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St160 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn160;
CREATE TABLE taxish20150405_staggn160(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn160
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St160 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1160;
CREATE TABLE taxish20150405_agg1160(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St160) bins
INSERT OVERWRITE TABLE taxish20150405_agg1160
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2160;
CREATE TABLE taxish20150405_agg2160(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St160) bins
INSERT OVERWRITE TABLE taxish20150405_agg2160
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1160
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","160",MIN(c),MAX(c);
FROM taxish20150405_agg2160
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","160",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time165;
CREATE TABLE taxish20150405_time165(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 16:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time165
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 17:00:00';

DROP TABLE IF EXISTS taxish20150405_St165;
CREATE EXTERNAL TABLE taxish20150405_St165(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St165
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time165 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time165;
DROP TABLE IF EXISTS taxish20150405_Stmax165;
CREATE TABLE taxish20150405_Stmax165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin165;
CREATE TABLE taxish20150405_Stmin165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St165 t GROUP BY t.carId) ts,taxish20150405_St165 t
INSERT OVERWRITE TABLE taxish20150405_Stmax165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St165 t GROUP BY t.carId) ts,taxish20150405_St165 t
INSERT OVERWRITE TABLE taxish20150405_Stmin165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp165;
CREATE TABLE taxish20150405_STODp165(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin165 tmin,taxish20150405_Stmax165 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp165
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf165;
CREATE TABLE taxish20150405_STODf165(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf165
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp165 od1 JOIN taxish20150405_STODp165 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD165;
CREATE TABLE taxish20150405_STOD165(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD165
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf165
WHERE count>0;
drop table taxish20150405_Stmax165;
drop table taxish20150405_Stmin165;
drop table taxish20150405_STODf165;
FROM taxish20150405_STOD165
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","165",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO165;
CREATE TABLE taxish20150405_STO165(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD165;
CREATE TABLE taxish20150405_STD165(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO165
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp165
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD165
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp165
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP165;
CREATE TABLE taxish20150405_STTP165(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO165 o,taxish20150405_STD165 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP165
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO165;
drop table taxish20150405_STD165;
drop table taxish20150405_STODp165;
DROP TABLE IF EXISTS taxish20150405_STTPn165;
CREATE TABLE taxish20150405_STTPn165(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP165 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn165
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg165;
CREATE TABLE taxish20150405_stagg165(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg165
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St165 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn165;
CREATE TABLE taxish20150405_staggn165(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn165
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St165 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1165;
CREATE TABLE taxish20150405_agg1165(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St165) bins
INSERT OVERWRITE TABLE taxish20150405_agg1165
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2165;
CREATE TABLE taxish20150405_agg2165(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St165) bins
INSERT OVERWRITE TABLE taxish20150405_agg2165
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1165
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","165",MIN(c),MAX(c);
FROM taxish20150405_agg2165
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","165",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time170;
CREATE TABLE taxish20150405_time170(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 17:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time170
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 17:30:00';

DROP TABLE IF EXISTS taxish20150405_St170;
CREATE EXTERNAL TABLE taxish20150405_St170(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St170
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time170 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time170;
DROP TABLE IF EXISTS taxish20150405_Stmax170;
CREATE TABLE taxish20150405_Stmax170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin170;
CREATE TABLE taxish20150405_Stmin170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St170 t GROUP BY t.carId) ts,taxish20150405_St170 t
INSERT OVERWRITE TABLE taxish20150405_Stmax170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St170 t GROUP BY t.carId) ts,taxish20150405_St170 t
INSERT OVERWRITE TABLE taxish20150405_Stmin170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp170;
CREATE TABLE taxish20150405_STODp170(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin170 tmin,taxish20150405_Stmax170 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp170
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf170;
CREATE TABLE taxish20150405_STODf170(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf170
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp170 od1 JOIN taxish20150405_STODp170 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD170;
CREATE TABLE taxish20150405_STOD170(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD170
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf170
WHERE count>0;
drop table taxish20150405_Stmax170;
drop table taxish20150405_Stmin170;
drop table taxish20150405_STODf170;
FROM taxish20150405_STOD170
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","170",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO170;
CREATE TABLE taxish20150405_STO170(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD170;
CREATE TABLE taxish20150405_STD170(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO170
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp170
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD170
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp170
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP170;
CREATE TABLE taxish20150405_STTP170(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO170 o,taxish20150405_STD170 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP170
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO170;
drop table taxish20150405_STD170;
drop table taxish20150405_STODp170;
DROP TABLE IF EXISTS taxish20150405_STTPn170;
CREATE TABLE taxish20150405_STTPn170(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP170 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn170
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg170;
CREATE TABLE taxish20150405_stagg170(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg170
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St170 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn170;
CREATE TABLE taxish20150405_staggn170(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn170
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St170 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1170;
CREATE TABLE taxish20150405_agg1170(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St170) bins
INSERT OVERWRITE TABLE taxish20150405_agg1170
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2170;
CREATE TABLE taxish20150405_agg2170(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St170) bins
INSERT OVERWRITE TABLE taxish20150405_agg2170
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1170
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","170",MIN(c),MAX(c);
FROM taxish20150405_agg2170
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","170",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time175;
CREATE TABLE taxish20150405_time175(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 17:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time175
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 18:00:00';

DROP TABLE IF EXISTS taxish20150405_St175;
CREATE EXTERNAL TABLE taxish20150405_St175(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St175
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time175 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time175;
DROP TABLE IF EXISTS taxish20150405_Stmax175;
CREATE TABLE taxish20150405_Stmax175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin175;
CREATE TABLE taxish20150405_Stmin175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St175 t GROUP BY t.carId) ts,taxish20150405_St175 t
INSERT OVERWRITE TABLE taxish20150405_Stmax175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St175 t GROUP BY t.carId) ts,taxish20150405_St175 t
INSERT OVERWRITE TABLE taxish20150405_Stmin175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp175;
CREATE TABLE taxish20150405_STODp175(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin175 tmin,taxish20150405_Stmax175 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp175
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf175;
CREATE TABLE taxish20150405_STODf175(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf175
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp175 od1 JOIN taxish20150405_STODp175 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD175;
CREATE TABLE taxish20150405_STOD175(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD175
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf175
WHERE count>0;
drop table taxish20150405_Stmax175;
drop table taxish20150405_Stmin175;
drop table taxish20150405_STODf175;
FROM taxish20150405_STOD175
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","175",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO175;
CREATE TABLE taxish20150405_STO175(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD175;
CREATE TABLE taxish20150405_STD175(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO175
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp175
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD175
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp175
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP175;
CREATE TABLE taxish20150405_STTP175(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO175 o,taxish20150405_STD175 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP175
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO175;
drop table taxish20150405_STD175;
drop table taxish20150405_STODp175;
DROP TABLE IF EXISTS taxish20150405_STTPn175;
CREATE TABLE taxish20150405_STTPn175(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP175 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn175
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg175;
CREATE TABLE taxish20150405_stagg175(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg175
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St175 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn175;
CREATE TABLE taxish20150405_staggn175(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn175
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St175 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1175;
CREATE TABLE taxish20150405_agg1175(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St175) bins
INSERT OVERWRITE TABLE taxish20150405_agg1175
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2175;
CREATE TABLE taxish20150405_agg2175(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St175) bins
INSERT OVERWRITE TABLE taxish20150405_agg2175
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1175
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","175",MIN(c),MAX(c);
FROM taxish20150405_agg2175
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","175",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time180;
CREATE TABLE taxish20150405_time180(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 18:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time180
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 18:30:00';

DROP TABLE IF EXISTS taxish20150405_St180;
CREATE EXTERNAL TABLE taxish20150405_St180(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St180
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time180 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time180;
DROP TABLE IF EXISTS taxish20150405_Stmax180;
CREATE TABLE taxish20150405_Stmax180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin180;
CREATE TABLE taxish20150405_Stmin180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St180 t GROUP BY t.carId) ts,taxish20150405_St180 t
INSERT OVERWRITE TABLE taxish20150405_Stmax180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St180 t GROUP BY t.carId) ts,taxish20150405_St180 t
INSERT OVERWRITE TABLE taxish20150405_Stmin180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp180;
CREATE TABLE taxish20150405_STODp180(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin180 tmin,taxish20150405_Stmax180 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp180
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf180;
CREATE TABLE taxish20150405_STODf180(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf180
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp180 od1 JOIN taxish20150405_STODp180 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD180;
CREATE TABLE taxish20150405_STOD180(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD180
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf180
WHERE count>0;
drop table taxish20150405_Stmax180;
drop table taxish20150405_Stmin180;
drop table taxish20150405_STODf180;
FROM taxish20150405_STOD180
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","180",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO180;
CREATE TABLE taxish20150405_STO180(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD180;
CREATE TABLE taxish20150405_STD180(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO180
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp180
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD180
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp180
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP180;
CREATE TABLE taxish20150405_STTP180(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO180 o,taxish20150405_STD180 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP180
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO180;
drop table taxish20150405_STD180;
drop table taxish20150405_STODp180;
DROP TABLE IF EXISTS taxish20150405_STTPn180;
CREATE TABLE taxish20150405_STTPn180(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP180 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn180
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg180;
CREATE TABLE taxish20150405_stagg180(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg180
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St180 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn180;
CREATE TABLE taxish20150405_staggn180(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn180
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St180 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1180;
CREATE TABLE taxish20150405_agg1180(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St180) bins
INSERT OVERWRITE TABLE taxish20150405_agg1180
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2180;
CREATE TABLE taxish20150405_agg2180(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St180) bins
INSERT OVERWRITE TABLE taxish20150405_agg2180
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1180
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","180",MIN(c),MAX(c);
FROM taxish20150405_agg2180
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","180",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time185;
CREATE TABLE taxish20150405_time185(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 18:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time185
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 19:00:00';

DROP TABLE IF EXISTS taxish20150405_St185;
CREATE EXTERNAL TABLE taxish20150405_St185(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St185
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time185 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time185;
DROP TABLE IF EXISTS taxish20150405_Stmax185;
CREATE TABLE taxish20150405_Stmax185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin185;
CREATE TABLE taxish20150405_Stmin185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St185 t GROUP BY t.carId) ts,taxish20150405_St185 t
INSERT OVERWRITE TABLE taxish20150405_Stmax185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St185 t GROUP BY t.carId) ts,taxish20150405_St185 t
INSERT OVERWRITE TABLE taxish20150405_Stmin185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp185;
CREATE TABLE taxish20150405_STODp185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin185 tmin,taxish20150405_Stmax185 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp185
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf185;
CREATE TABLE taxish20150405_STODf185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf185
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp185 od1 JOIN taxish20150405_STODp185 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD185;
CREATE TABLE taxish20150405_STOD185(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD185
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf185
WHERE count>0;
drop table taxish20150405_Stmax185;
drop table taxish20150405_Stmin185;
drop table taxish20150405_STODf185;
FROM taxish20150405_STOD185
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","185",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO185;
CREATE TABLE taxish20150405_STO185(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD185;
CREATE TABLE taxish20150405_STD185(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO185
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp185
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD185
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp185
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP185;
CREATE TABLE taxish20150405_STTP185(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO185 o,taxish20150405_STD185 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP185
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO185;
drop table taxish20150405_STD185;
drop table taxish20150405_STODp185;
DROP TABLE IF EXISTS taxish20150405_STTPn185;
CREATE TABLE taxish20150405_STTPn185(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP185 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn185
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg185;
CREATE TABLE taxish20150405_stagg185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg185
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St185 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn185;
CREATE TABLE taxish20150405_staggn185(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn185
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St185 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1185;
CREATE TABLE taxish20150405_agg1185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St185) bins
INSERT OVERWRITE TABLE taxish20150405_agg1185
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2185;
CREATE TABLE taxish20150405_agg2185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St185) bins
INSERT OVERWRITE TABLE taxish20150405_agg2185
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1185
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","185",MIN(c),MAX(c);
FROM taxish20150405_agg2185
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","185",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time190;
CREATE TABLE taxish20150405_time190(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 19:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time190
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 19:30:00';

DROP TABLE IF EXISTS taxish20150405_St190;
CREATE EXTERNAL TABLE taxish20150405_St190(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St190
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time190 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time190;
DROP TABLE IF EXISTS taxish20150405_Stmax190;
CREATE TABLE taxish20150405_Stmax190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin190;
CREATE TABLE taxish20150405_Stmin190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St190 t GROUP BY t.carId) ts,taxish20150405_St190 t
INSERT OVERWRITE TABLE taxish20150405_Stmax190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St190 t GROUP BY t.carId) ts,taxish20150405_St190 t
INSERT OVERWRITE TABLE taxish20150405_Stmin190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp190;
CREATE TABLE taxish20150405_STODp190(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin190 tmin,taxish20150405_Stmax190 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp190
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf190;
CREATE TABLE taxish20150405_STODf190(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf190
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp190 od1 JOIN taxish20150405_STODp190 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD190;
CREATE TABLE taxish20150405_STOD190(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD190
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf190
WHERE count>0;
drop table taxish20150405_Stmax190;
drop table taxish20150405_Stmin190;
drop table taxish20150405_STODf190;
FROM taxish20150405_STOD190
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","190",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO190;
CREATE TABLE taxish20150405_STO190(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD190;
CREATE TABLE taxish20150405_STD190(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO190
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp190
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD190
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp190
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP190;
CREATE TABLE taxish20150405_STTP190(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO190 o,taxish20150405_STD190 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP190
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO190;
drop table taxish20150405_STD190;
drop table taxish20150405_STODp190;
DROP TABLE IF EXISTS taxish20150405_STTPn190;
CREATE TABLE taxish20150405_STTPn190(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP190 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn190
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg190;
CREATE TABLE taxish20150405_stagg190(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg190
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St190 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn190;
CREATE TABLE taxish20150405_staggn190(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn190
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St190 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1190;
CREATE TABLE taxish20150405_agg1190(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St190) bins
INSERT OVERWRITE TABLE taxish20150405_agg1190
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2190;
CREATE TABLE taxish20150405_agg2190(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St190) bins
INSERT OVERWRITE TABLE taxish20150405_agg2190
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1190
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","190",MIN(c),MAX(c);
FROM taxish20150405_agg2190
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","190",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time195;
CREATE TABLE taxish20150405_time195(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 19:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time195
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 20:00:00';

DROP TABLE IF EXISTS taxish20150405_St195;
CREATE EXTERNAL TABLE taxish20150405_St195(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St195
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time195 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time195;
DROP TABLE IF EXISTS taxish20150405_Stmax195;
CREATE TABLE taxish20150405_Stmax195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin195;
CREATE TABLE taxish20150405_Stmin195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St195 t GROUP BY t.carId) ts,taxish20150405_St195 t
INSERT OVERWRITE TABLE taxish20150405_Stmax195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St195 t GROUP BY t.carId) ts,taxish20150405_St195 t
INSERT OVERWRITE TABLE taxish20150405_Stmin195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp195;
CREATE TABLE taxish20150405_STODp195(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin195 tmin,taxish20150405_Stmax195 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp195
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf195;
CREATE TABLE taxish20150405_STODf195(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf195
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp195 od1 JOIN taxish20150405_STODp195 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD195;
CREATE TABLE taxish20150405_STOD195(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD195
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf195
WHERE count>0;
drop table taxish20150405_Stmax195;
drop table taxish20150405_Stmin195;
drop table taxish20150405_STODf195;
FROM taxish20150405_STOD195
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","195",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO195;
CREATE TABLE taxish20150405_STO195(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD195;
CREATE TABLE taxish20150405_STD195(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO195
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp195
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD195
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp195
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP195;
CREATE TABLE taxish20150405_STTP195(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO195 o,taxish20150405_STD195 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP195
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO195;
drop table taxish20150405_STD195;
drop table taxish20150405_STODp195;
DROP TABLE IF EXISTS taxish20150405_STTPn195;
CREATE TABLE taxish20150405_STTPn195(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP195 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn195
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg195;
CREATE TABLE taxish20150405_stagg195(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg195
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St195 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn195;
CREATE TABLE taxish20150405_staggn195(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn195
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St195 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1195;
CREATE TABLE taxish20150405_agg1195(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St195) bins
INSERT OVERWRITE TABLE taxish20150405_agg1195
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2195;
CREATE TABLE taxish20150405_agg2195(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St195) bins
INSERT OVERWRITE TABLE taxish20150405_agg2195
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1195
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","195",MIN(c),MAX(c);
FROM taxish20150405_agg2195
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","195",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time200;
CREATE TABLE taxish20150405_time200(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 20:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time200
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 20:30:00';

DROP TABLE IF EXISTS taxish20150405_St200;
CREATE EXTERNAL TABLE taxish20150405_St200(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St200
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time200 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time200;
DROP TABLE IF EXISTS taxish20150405_Stmax200;
CREATE TABLE taxish20150405_Stmax200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin200;
CREATE TABLE taxish20150405_Stmin200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St200 t GROUP BY t.carId) ts,taxish20150405_St200 t
INSERT OVERWRITE TABLE taxish20150405_Stmax200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St200 t GROUP BY t.carId) ts,taxish20150405_St200 t
INSERT OVERWRITE TABLE taxish20150405_Stmin200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp200;
CREATE TABLE taxish20150405_STODp200(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin200 tmin,taxish20150405_Stmax200 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp200
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf200;
CREATE TABLE taxish20150405_STODf200(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf200
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp200 od1 JOIN taxish20150405_STODp200 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD200;
CREATE TABLE taxish20150405_STOD200(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD200
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf200
WHERE count>0;
drop table taxish20150405_Stmax200;
drop table taxish20150405_Stmin200;
drop table taxish20150405_STODf200;
FROM taxish20150405_STOD200
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","200",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO200;
CREATE TABLE taxish20150405_STO200(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD200;
CREATE TABLE taxish20150405_STD200(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO200
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp200
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD200
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp200
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP200;
CREATE TABLE taxish20150405_STTP200(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO200 o,taxish20150405_STD200 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP200
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO200;
drop table taxish20150405_STD200;
drop table taxish20150405_STODp200;
DROP TABLE IF EXISTS taxish20150405_STTPn200;
CREATE TABLE taxish20150405_STTPn200(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP200 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn200
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg200;
CREATE TABLE taxish20150405_stagg200(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg200
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St200 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn200;
CREATE TABLE taxish20150405_staggn200(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn200
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St200 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1200;
CREATE TABLE taxish20150405_agg1200(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St200) bins
INSERT OVERWRITE TABLE taxish20150405_agg1200
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2200;
CREATE TABLE taxish20150405_agg2200(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St200) bins
INSERT OVERWRITE TABLE taxish20150405_agg2200
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1200
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","200",MIN(c),MAX(c);
FROM taxish20150405_agg2200
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","200",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time205;
CREATE TABLE taxish20150405_time205(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 20:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time205
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 21:00:00';

DROP TABLE IF EXISTS taxish20150405_St205;
CREATE EXTERNAL TABLE taxish20150405_St205(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St205
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time205 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time205;
DROP TABLE IF EXISTS taxish20150405_Stmax205;
CREATE TABLE taxish20150405_Stmax205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin205;
CREATE TABLE taxish20150405_Stmin205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St205 t GROUP BY t.carId) ts,taxish20150405_St205 t
INSERT OVERWRITE TABLE taxish20150405_Stmax205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St205 t GROUP BY t.carId) ts,taxish20150405_St205 t
INSERT OVERWRITE TABLE taxish20150405_Stmin205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp205;
CREATE TABLE taxish20150405_STODp205(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin205 tmin,taxish20150405_Stmax205 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp205
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf205;
CREATE TABLE taxish20150405_STODf205(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf205
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp205 od1 JOIN taxish20150405_STODp205 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD205;
CREATE TABLE taxish20150405_STOD205(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD205
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf205
WHERE count>0;
drop table taxish20150405_Stmax205;
drop table taxish20150405_Stmin205;
drop table taxish20150405_STODf205;
FROM taxish20150405_STOD205
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","205",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO205;
CREATE TABLE taxish20150405_STO205(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD205;
CREATE TABLE taxish20150405_STD205(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO205
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp205
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD205
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp205
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP205;
CREATE TABLE taxish20150405_STTP205(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO205 o,taxish20150405_STD205 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP205
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO205;
drop table taxish20150405_STD205;
drop table taxish20150405_STODp205;
DROP TABLE IF EXISTS taxish20150405_STTPn205;
CREATE TABLE taxish20150405_STTPn205(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP205 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn205
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg205;
CREATE TABLE taxish20150405_stagg205(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg205
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St205 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn205;
CREATE TABLE taxish20150405_staggn205(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn205
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St205 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1205;
CREATE TABLE taxish20150405_agg1205(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St205) bins
INSERT OVERWRITE TABLE taxish20150405_agg1205
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2205;
CREATE TABLE taxish20150405_agg2205(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St205) bins
INSERT OVERWRITE TABLE taxish20150405_agg2205
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1205
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","205",MIN(c),MAX(c);
FROM taxish20150405_agg2205
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","205",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time210;
CREATE TABLE taxish20150405_time210(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 21:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time210
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 21:30:00';

DROP TABLE IF EXISTS taxish20150405_St210;
CREATE EXTERNAL TABLE taxish20150405_St210(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St210
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time210 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time210;
DROP TABLE IF EXISTS taxish20150405_Stmax210;
CREATE TABLE taxish20150405_Stmax210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin210;
CREATE TABLE taxish20150405_Stmin210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St210 t GROUP BY t.carId) ts,taxish20150405_St210 t
INSERT OVERWRITE TABLE taxish20150405_Stmax210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St210 t GROUP BY t.carId) ts,taxish20150405_St210 t
INSERT OVERWRITE TABLE taxish20150405_Stmin210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp210;
CREATE TABLE taxish20150405_STODp210(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin210 tmin,taxish20150405_Stmax210 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp210
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf210;
CREATE TABLE taxish20150405_STODf210(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf210
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp210 od1 JOIN taxish20150405_STODp210 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD210;
CREATE TABLE taxish20150405_STOD210(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD210
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf210
WHERE count>0;
drop table taxish20150405_Stmax210;
drop table taxish20150405_Stmin210;
drop table taxish20150405_STODf210;
FROM taxish20150405_STOD210
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","210",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO210;
CREATE TABLE taxish20150405_STO210(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD210;
CREATE TABLE taxish20150405_STD210(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO210
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp210
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD210
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp210
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP210;
CREATE TABLE taxish20150405_STTP210(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO210 o,taxish20150405_STD210 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP210
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO210;
drop table taxish20150405_STD210;
drop table taxish20150405_STODp210;
DROP TABLE IF EXISTS taxish20150405_STTPn210;
CREATE TABLE taxish20150405_STTPn210(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP210 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn210
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg210;
CREATE TABLE taxish20150405_stagg210(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg210
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St210 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn210;
CREATE TABLE taxish20150405_staggn210(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn210
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St210 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1210;
CREATE TABLE taxish20150405_agg1210(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St210) bins
INSERT OVERWRITE TABLE taxish20150405_agg1210
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2210;
CREATE TABLE taxish20150405_agg2210(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St210) bins
INSERT OVERWRITE TABLE taxish20150405_agg2210
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1210
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","210",MIN(c),MAX(c);
FROM taxish20150405_agg2210
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","210",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time215;
CREATE TABLE taxish20150405_time215(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 21:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time215
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 22:00:00';

DROP TABLE IF EXISTS taxish20150405_St215;
CREATE EXTERNAL TABLE taxish20150405_St215(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St215
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time215 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time215;
DROP TABLE IF EXISTS taxish20150405_Stmax215;
CREATE TABLE taxish20150405_Stmax215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin215;
CREATE TABLE taxish20150405_Stmin215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St215 t GROUP BY t.carId) ts,taxish20150405_St215 t
INSERT OVERWRITE TABLE taxish20150405_Stmax215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St215 t GROUP BY t.carId) ts,taxish20150405_St215 t
INSERT OVERWRITE TABLE taxish20150405_Stmin215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp215;
CREATE TABLE taxish20150405_STODp215(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin215 tmin,taxish20150405_Stmax215 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp215
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf215;
CREATE TABLE taxish20150405_STODf215(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf215
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp215 od1 JOIN taxish20150405_STODp215 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD215;
CREATE TABLE taxish20150405_STOD215(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD215
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf215
WHERE count>0;
drop table taxish20150405_Stmax215;
drop table taxish20150405_Stmin215;
drop table taxish20150405_STODf215;
FROM taxish20150405_STOD215
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","215",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO215;
CREATE TABLE taxish20150405_STO215(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD215;
CREATE TABLE taxish20150405_STD215(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO215
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp215
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD215
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp215
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP215;
CREATE TABLE taxish20150405_STTP215(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO215 o,taxish20150405_STD215 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP215
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO215;
drop table taxish20150405_STD215;
drop table taxish20150405_STODp215;
DROP TABLE IF EXISTS taxish20150405_STTPn215;
CREATE TABLE taxish20150405_STTPn215(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP215 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn215
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg215;
CREATE TABLE taxish20150405_stagg215(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg215
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St215 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn215;
CREATE TABLE taxish20150405_staggn215(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn215
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St215 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1215;
CREATE TABLE taxish20150405_agg1215(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St215) bins
INSERT OVERWRITE TABLE taxish20150405_agg1215
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2215;
CREATE TABLE taxish20150405_agg2215(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St215) bins
INSERT OVERWRITE TABLE taxish20150405_agg2215
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1215
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","215",MIN(c),MAX(c);
FROM taxish20150405_agg2215
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","215",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time220;
CREATE TABLE taxish20150405_time220(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 22:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time220
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 22:30:00';

DROP TABLE IF EXISTS taxish20150405_St220;
CREATE EXTERNAL TABLE taxish20150405_St220(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St220
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time220 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time220;
DROP TABLE IF EXISTS taxish20150405_Stmax220;
CREATE TABLE taxish20150405_Stmax220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin220;
CREATE TABLE taxish20150405_Stmin220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St220 t GROUP BY t.carId) ts,taxish20150405_St220 t
INSERT OVERWRITE TABLE taxish20150405_Stmax220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St220 t GROUP BY t.carId) ts,taxish20150405_St220 t
INSERT OVERWRITE TABLE taxish20150405_Stmin220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp220;
CREATE TABLE taxish20150405_STODp220(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin220 tmin,taxish20150405_Stmax220 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp220
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf220;
CREATE TABLE taxish20150405_STODf220(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf220
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp220 od1 JOIN taxish20150405_STODp220 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD220;
CREATE TABLE taxish20150405_STOD220(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD220
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf220
WHERE count>0;
drop table taxish20150405_Stmax220;
drop table taxish20150405_Stmin220;
drop table taxish20150405_STODf220;
FROM taxish20150405_STOD220
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","220",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO220;
CREATE TABLE taxish20150405_STO220(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD220;
CREATE TABLE taxish20150405_STD220(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO220
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp220
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD220
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp220
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP220;
CREATE TABLE taxish20150405_STTP220(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO220 o,taxish20150405_STD220 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP220
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO220;
drop table taxish20150405_STD220;
drop table taxish20150405_STODp220;
DROP TABLE IF EXISTS taxish20150405_STTPn220;
CREATE TABLE taxish20150405_STTPn220(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP220 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn220
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg220;
CREATE TABLE taxish20150405_stagg220(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg220
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St220 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn220;
CREATE TABLE taxish20150405_staggn220(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn220
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St220 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1220;
CREATE TABLE taxish20150405_agg1220(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St220) bins
INSERT OVERWRITE TABLE taxish20150405_agg1220
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2220;
CREATE TABLE taxish20150405_agg2220(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St220) bins
INSERT OVERWRITE TABLE taxish20150405_agg2220
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1220
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","220",MIN(c),MAX(c);
FROM taxish20150405_agg2220
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","220",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time225;
CREATE TABLE taxish20150405_time225(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 22:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time225
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 23:00:00';

DROP TABLE IF EXISTS taxish20150405_St225;
CREATE EXTERNAL TABLE taxish20150405_St225(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St225
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time225 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time225;
DROP TABLE IF EXISTS taxish20150405_Stmax225;
CREATE TABLE taxish20150405_Stmax225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin225;
CREATE TABLE taxish20150405_Stmin225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St225 t GROUP BY t.carId) ts,taxish20150405_St225 t
INSERT OVERWRITE TABLE taxish20150405_Stmax225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St225 t GROUP BY t.carId) ts,taxish20150405_St225 t
INSERT OVERWRITE TABLE taxish20150405_Stmin225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp225;
CREATE TABLE taxish20150405_STODp225(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin225 tmin,taxish20150405_Stmax225 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp225
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf225;
CREATE TABLE taxish20150405_STODf225(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf225
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp225 od1 JOIN taxish20150405_STODp225 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD225;
CREATE TABLE taxish20150405_STOD225(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD225
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf225
WHERE count>0;
drop table taxish20150405_Stmax225;
drop table taxish20150405_Stmin225;
drop table taxish20150405_STODf225;
FROM taxish20150405_STOD225
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","225",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO225;
CREATE TABLE taxish20150405_STO225(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD225;
CREATE TABLE taxish20150405_STD225(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO225
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp225
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD225
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp225
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP225;
CREATE TABLE taxish20150405_STTP225(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO225 o,taxish20150405_STD225 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP225
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO225;
drop table taxish20150405_STD225;
drop table taxish20150405_STODp225;
DROP TABLE IF EXISTS taxish20150405_STTPn225;
CREATE TABLE taxish20150405_STTPn225(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP225 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn225
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg225;
CREATE TABLE taxish20150405_stagg225(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg225
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St225 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn225;
CREATE TABLE taxish20150405_staggn225(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn225
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St225 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1225;
CREATE TABLE taxish20150405_agg1225(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St225) bins
INSERT OVERWRITE TABLE taxish20150405_agg1225
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2225;
CREATE TABLE taxish20150405_agg2225(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St225) bins
INSERT OVERWRITE TABLE taxish20150405_agg2225
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1225
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","225",MIN(c),MAX(c);
FROM taxish20150405_agg2225
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","225",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time230;
CREATE TABLE taxish20150405_time230(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 23:00:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time230
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 23:30:00';

DROP TABLE IF EXISTS taxish20150405_St230;
CREATE EXTERNAL TABLE taxish20150405_St230(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St230
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time230 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time230;
DROP TABLE IF EXISTS taxish20150405_Stmax230;
CREATE TABLE taxish20150405_Stmax230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin230;
CREATE TABLE taxish20150405_Stmin230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St230 t GROUP BY t.carId) ts,taxish20150405_St230 t
INSERT OVERWRITE TABLE taxish20150405_Stmax230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St230 t GROUP BY t.carId) ts,taxish20150405_St230 t
INSERT OVERWRITE TABLE taxish20150405_Stmin230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp230;
CREATE TABLE taxish20150405_STODp230(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin230 tmin,taxish20150405_Stmax230 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp230
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf230;
CREATE TABLE taxish20150405_STODf230(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf230
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp230 od1 JOIN taxish20150405_STODp230 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD230;
CREATE TABLE taxish20150405_STOD230(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD230
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf230
WHERE count>0;
drop table taxish20150405_Stmax230;
drop table taxish20150405_Stmin230;
drop table taxish20150405_STODf230;
FROM taxish20150405_STOD230
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","230",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO230;
CREATE TABLE taxish20150405_STO230(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD230;
CREATE TABLE taxish20150405_STD230(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO230
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp230
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD230
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp230
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP230;
CREATE TABLE taxish20150405_STTP230(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO230 o,taxish20150405_STD230 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP230
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO230;
drop table taxish20150405_STD230;
drop table taxish20150405_STODp230;
DROP TABLE IF EXISTS taxish20150405_STTPn230;
CREATE TABLE taxish20150405_STTPn230(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP230 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn230
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg230;
CREATE TABLE taxish20150405_stagg230(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg230
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St230 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn230;
CREATE TABLE taxish20150405_staggn230(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn230
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St230 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1230;
CREATE TABLE taxish20150405_agg1230(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St230) bins
INSERT OVERWRITE TABLE taxish20150405_agg1230
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2230;
CREATE TABLE taxish20150405_agg2230(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St230) bins
INSERT OVERWRITE TABLE taxish20150405_agg2230
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1230
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","230",MIN(c),MAX(c);
FROM taxish20150405_agg2230
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","230",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_time235;
CREATE TABLE taxish20150405_time235(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150405_ WHERE taxish20150405_.receiveTime > '2015-04-05 23:30:00') taxishs
INSERT OVERWRITE TABLE taxish20150405_time235
SELECT *
WHERE taxishs.receiveTime < '2015-04-05 24:00:00';

DROP TABLE IF EXISTS taxish20150405_St235;
CREATE EXTERNAL TABLE taxish20150405_St235(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_St235
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150405_time235 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150405_time235;
DROP TABLE IF EXISTS taxish20150405_Stmax235;
CREATE TABLE taxish20150405_Stmax235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150405_Stmin235;
CREATE TABLE taxish20150405_Stmin235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150405_St235 t GROUP BY t.carId) ts,taxish20150405_St235 t
INSERT OVERWRITE TABLE taxish20150405_Stmax235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150405_St235 t GROUP BY t.carId) ts,taxish20150405_St235 t
INSERT OVERWRITE TABLE taxish20150405_Stmin235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150405_STODp235;
CREATE TABLE taxish20150405_STODp235(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150405_Stmin235 tmin,taxish20150405_Stmax235 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150405_STODp235
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150405_STODf235;
CREATE TABLE taxish20150405_STODf235(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STODf235
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150405_STODp235 od1 JOIN taxish20150405_STODp235 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150405_STOD235;
CREATE TABLE taxish20150405_STOD235(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_STOD235
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150405_STODf235
WHERE count>0;
drop table taxish20150405_Stmax235;
drop table taxish20150405_Stmin235;
drop table taxish20150405_STODf235;
FROM taxish20150405_STOD235
INSERT INTO TABLE taxish20150405_valuepp
SELECT "STOD","235",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150405_STO235;
CREATE TABLE taxish20150405_STO235(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150405_STD235;
CREATE TABLE taxish20150405_STD235(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150405_STO235
SELECT OctOBJECTID,SUM(count)
FROM taxish20150405_STODp235
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150405_STD235
SELECT DctOBJECTID,SUM(count)
FROM taxish20150405_STODp235
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150405_STTP235;
CREATE TABLE taxish20150405_STTP235(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150405_STO235 o,taxish20150405_STD235 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTP235
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150405_STO235;
drop table taxish20150405_STD235;
drop table taxish20150405_STODp235;
DROP TABLE IF EXISTS taxish20150405_STTPn235;
CREATE TABLE taxish20150405_STTPn235(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150405_STTP235 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150405_STTPn235
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150405_stagg235;
CREATE TABLE taxish20150405_stagg235(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150405_stagg235
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St235 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
DROP TABLE IF EXISTS taxish20150405_staggn235;
CREATE TABLE taxish20150405_staggn235(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150405_staggn235
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150405_St235 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150405_agg1235;
CREATE TABLE taxish20150405_agg1235(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St235) bins
INSERT OVERWRITE TABLE taxish20150405_agg1235
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150405_agg2235;
CREATE TABLE taxish20150405_agg2235(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150405_St235) bins
INSERT OVERWRITE TABLE taxish20150405_agg2235
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150405_agg1235
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG1","235",MIN(c),MAX(c);
FROM taxish20150405_agg2235
INSERT INTO TABLE taxish20150405_valuepp
SELECT "AGG2","235",MIN(c),MAX(c);

INSERT OVERWRITE TABLE taxish20150405_value 
SELECT * FROM taxish20150405_valuepp;
drop table taxish20150405_valuepp;
