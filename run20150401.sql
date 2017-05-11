add jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar
    /root/esri-git/json-serde-1.3.8-jar-with-dependencies.jar
    /root/esri-git/json-udf-1.3.8-jar-with-dependencies.jar;
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';

DROP TABLE IF EXISTS blocksh_v1p;
CREATE EXTERNAL TABLE blocksh_v1p (Name string, objectid string, cx DOUBLE,cy DOUBLE,BoundaryShape binary)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
LOAD DATA INPATH  '/SHstreet/newEnCounty.json' OVERWRITE INTO TABLE blocksh_v1p;

DROP TABLE IF EXISTS taxish20150401_;
CREATE EXTERNAL TABLE taxish20150401_(carId DOUBLE,isAlarm DOUBLE,isEmpty DOUBLE,topLight DOUBLE,
Elevated DOUBLE,isBrake DOUBLE,receiveTime TIMESTAMP,GPSTime STRING,longitude DOUBLE,latitude DOUBLE,
speed DOUBLE,direction DOUBLE,satellite DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile
tblproperties ("skip.header.line.count"="1");
describe taxish20150401_;
LOAD DATA INPATH '/taxidemo/part-4.01.csv' OVERWRITE INTO TABLE taxish20150401_;

DROP TABLE IF EXISTS taxish20150401_value;
CREATE EXTERNAL TABLE taxish20150401_value(p STRING,m STRING,n DOUBLE,x DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
DROP TABLE IF EXISTS taxish20150401_valuep;
CREATE TABLE taxish20150401_valuep(type STRING,time STRING,min DOUBLE,max DOUBLE);

DROP TABLE IF EXISTS taxish20150401_time000;
CREATE TABLE taxish20150401_time000(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 00:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time000
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 00:30:00';

DROP TABLE IF EXISTS taxish20150401_St000;
CREATE EXTERNAL TABLE taxish20150401_St000(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St000
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time000 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time000;
DROP TABLE IF EXISTS taxish20150401_Stmax000;
CREATE TABLE taxish20150401_Stmax000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin000;
CREATE TABLE taxish20150401_Stmin000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St000 t GROUP BY t.carId) ts,taxish20150401_St000 t
INSERT OVERWRITE TABLE taxish20150401_Stmax000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St000 t GROUP BY t.carId) ts,taxish20150401_St000 t
INSERT OVERWRITE TABLE taxish20150401_Stmin000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp000;
CREATE TABLE taxish20150401_STODp000(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin000 tmin,taxish20150401_Stmax000 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp000
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf000;
CREATE TABLE taxish20150401_STODf000(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf000
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp000 od1 JOIN taxish20150401_STODp000 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD000;
CREATE TABLE taxish20150401_STOD000(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD000
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf000
WHERE count>0;
drop table taxish20150401_Stmax000;
drop table taxish20150401_Stmin000;
drop table taxish20150401_STODf000;
FROM taxish20150401_STOD000
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","000",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO000;
CREATE TABLE taxish20150401_STO000(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD000;
CREATE TABLE taxish20150401_STD000(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO000
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp000
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD000
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp000
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP000;
CREATE TABLE taxish20150401_STTP000(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO000 o,taxish20150401_STD000 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP000
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO000;
drop table taxish20150401_STD000;
drop table taxish20150401_STODp000;
FROM taxish20150401_STTP000
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","000",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg000;
CREATE TABLE taxish20150401_stagg000(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg000
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St000 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg000
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","000",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1000;
CREATE TABLE taxish20150401_agg1000(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St000) bins
INSERT OVERWRITE TABLE taxish20150401_agg1000
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2000;
CREATE TABLE taxish20150401_agg2000(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St000) bins
INSERT OVERWRITE TABLE taxish20150401_agg2000
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1000
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","000",MIN(c),MAX(c);
FROM taxish20150401_agg2000
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","000",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time005;
CREATE TABLE taxish20150401_time005(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 00:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time005
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 01:00:00';

DROP TABLE IF EXISTS taxish20150401_St005;
CREATE EXTERNAL TABLE taxish20150401_St005(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St005
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time005 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time005;
DROP TABLE IF EXISTS taxish20150401_Stmax005;
CREATE TABLE taxish20150401_Stmax005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin005;
CREATE TABLE taxish20150401_Stmin005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St005 t GROUP BY t.carId) ts,taxish20150401_St005 t
INSERT OVERWRITE TABLE taxish20150401_Stmax005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St005 t GROUP BY t.carId) ts,taxish20150401_St005 t
INSERT OVERWRITE TABLE taxish20150401_Stmin005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp005;
CREATE TABLE taxish20150401_STODp005(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin005 tmin,taxish20150401_Stmax005 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp005
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf005;
CREATE TABLE taxish20150401_STODf005(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf005
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp005 od1 JOIN taxish20150401_STODp005 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD005;
CREATE TABLE taxish20150401_STOD005(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD005
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf005
WHERE count>0;
drop table taxish20150401_Stmax005;
drop table taxish20150401_Stmin005;
drop table taxish20150401_STODf005;
FROM taxish20150401_STOD005
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","005",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO005;
CREATE TABLE taxish20150401_STO005(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD005;
CREATE TABLE taxish20150401_STD005(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO005
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp005
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD005
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp005
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP005;
CREATE TABLE taxish20150401_STTP005(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO005 o,taxish20150401_STD005 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP005
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO005;
drop table taxish20150401_STD005;
drop table taxish20150401_STODp005;
FROM taxish20150401_STTP005
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","005",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg005;
CREATE TABLE taxish20150401_stagg005(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg005
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St005 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg005
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","005",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1005;
CREATE TABLE taxish20150401_agg1005(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St005) bins
INSERT OVERWRITE TABLE taxish20150401_agg1005
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2005;
CREATE TABLE taxish20150401_agg2005(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St005) bins
INSERT OVERWRITE TABLE taxish20150401_agg2005
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1005
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","005",MIN(c),MAX(c);
FROM taxish20150401_agg2005
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","005",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time010;
CREATE TABLE taxish20150401_time010(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 01:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time010
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 01:30:00';

DROP TABLE IF EXISTS taxish20150401_St010;
CREATE EXTERNAL TABLE taxish20150401_St010(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St010
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time010 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time010;
DROP TABLE IF EXISTS taxish20150401_Stmax010;
CREATE TABLE taxish20150401_Stmax010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin010;
CREATE TABLE taxish20150401_Stmin010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St010 t GROUP BY t.carId) ts,taxish20150401_St010 t
INSERT OVERWRITE TABLE taxish20150401_Stmax010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St010 t GROUP BY t.carId) ts,taxish20150401_St010 t
INSERT OVERWRITE TABLE taxish20150401_Stmin010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp010;
CREATE TABLE taxish20150401_STODp010(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin010 tmin,taxish20150401_Stmax010 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp010
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf010;
CREATE TABLE taxish20150401_STODf010(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf010
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp010 od1 JOIN taxish20150401_STODp010 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD010;
CREATE TABLE taxish20150401_STOD010(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD010
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf010
WHERE count>0;
drop table taxish20150401_Stmax010;
drop table taxish20150401_Stmin010;
drop table taxish20150401_STODf010;
FROM taxish20150401_STOD010
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","010",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO010;
CREATE TABLE taxish20150401_STO010(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD010;
CREATE TABLE taxish20150401_STD010(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO010
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp010
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD010
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp010
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP010;
CREATE TABLE taxish20150401_STTP010(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO010 o,taxish20150401_STD010 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP010
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO010;
drop table taxish20150401_STD010;
drop table taxish20150401_STODp010;
FROM taxish20150401_STTP010
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","010",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg010;
CREATE TABLE taxish20150401_stagg010(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg010
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St010 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg010
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","010",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1010;
CREATE TABLE taxish20150401_agg1010(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St010) bins
INSERT OVERWRITE TABLE taxish20150401_agg1010
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2010;
CREATE TABLE taxish20150401_agg2010(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St010) bins
INSERT OVERWRITE TABLE taxish20150401_agg2010
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1010
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","010",MIN(c),MAX(c);
FROM taxish20150401_agg2010
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","010",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time015;
CREATE TABLE taxish20150401_time015(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 01:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time015
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 02:00:00';

DROP TABLE IF EXISTS taxish20150401_St015;
CREATE EXTERNAL TABLE taxish20150401_St015(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St015
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time015 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time015;
DROP TABLE IF EXISTS taxish20150401_Stmax015;
CREATE TABLE taxish20150401_Stmax015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin015;
CREATE TABLE taxish20150401_Stmin015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St015 t GROUP BY t.carId) ts,taxish20150401_St015 t
INSERT OVERWRITE TABLE taxish20150401_Stmax015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St015 t GROUP BY t.carId) ts,taxish20150401_St015 t
INSERT OVERWRITE TABLE taxish20150401_Stmin015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp015;
CREATE TABLE taxish20150401_STODp015(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin015 tmin,taxish20150401_Stmax015 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp015
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf015;
CREATE TABLE taxish20150401_STODf015(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf015
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp015 od1 JOIN taxish20150401_STODp015 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD015;
CREATE TABLE taxish20150401_STOD015(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD015
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf015
WHERE count>0;
drop table taxish20150401_Stmax015;
drop table taxish20150401_Stmin015;
drop table taxish20150401_STODf015;
FROM taxish20150401_STOD015
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","015",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO015;
CREATE TABLE taxish20150401_STO015(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD015;
CREATE TABLE taxish20150401_STD015(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO015
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp015
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD015
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp015
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP015;
CREATE TABLE taxish20150401_STTP015(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO015 o,taxish20150401_STD015 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP015
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO015;
drop table taxish20150401_STD015;
drop table taxish20150401_STODp015;
FROM taxish20150401_STTP015
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","015",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg015;
CREATE TABLE taxish20150401_stagg015(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg015
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St015 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg015
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","015",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1015;
CREATE TABLE taxish20150401_agg1015(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St015) bins
INSERT OVERWRITE TABLE taxish20150401_agg1015
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2015;
CREATE TABLE taxish20150401_agg2015(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St015) bins
INSERT OVERWRITE TABLE taxish20150401_agg2015
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1015
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","015",MIN(c),MAX(c);
FROM taxish20150401_agg2015
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","015",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time020;
CREATE TABLE taxish20150401_time020(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 02:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time020
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 02:30:00';

DROP TABLE IF EXISTS taxish20150401_St020;
CREATE EXTERNAL TABLE taxish20150401_St020(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St020
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time020 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time020;
DROP TABLE IF EXISTS taxish20150401_Stmax020;
CREATE TABLE taxish20150401_Stmax020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin020;
CREATE TABLE taxish20150401_Stmin020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St020 t GROUP BY t.carId) ts,taxish20150401_St020 t
INSERT OVERWRITE TABLE taxish20150401_Stmax020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St020 t GROUP BY t.carId) ts,taxish20150401_St020 t
INSERT OVERWRITE TABLE taxish20150401_Stmin020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp020;
CREATE TABLE taxish20150401_STODp020(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin020 tmin,taxish20150401_Stmax020 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp020
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf020;
CREATE TABLE taxish20150401_STODf020(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf020
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp020 od1 JOIN taxish20150401_STODp020 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD020;
CREATE TABLE taxish20150401_STOD020(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD020
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf020
WHERE count>0;
drop table taxish20150401_Stmax020;
drop table taxish20150401_Stmin020;
drop table taxish20150401_STODf020;
FROM taxish20150401_STOD020
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","020",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO020;
CREATE TABLE taxish20150401_STO020(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD020;
CREATE TABLE taxish20150401_STD020(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO020
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp020
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD020
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp020
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP020;
CREATE TABLE taxish20150401_STTP020(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO020 o,taxish20150401_STD020 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP020
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO020;
drop table taxish20150401_STD020;
drop table taxish20150401_STODp020;
FROM taxish20150401_STTP020
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","020",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg020;
CREATE TABLE taxish20150401_stagg020(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg020
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St020 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg020
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","020",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1020;
CREATE TABLE taxish20150401_agg1020(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St020) bins
INSERT OVERWRITE TABLE taxish20150401_agg1020
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2020;
CREATE TABLE taxish20150401_agg2020(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St020) bins
INSERT OVERWRITE TABLE taxish20150401_agg2020
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1020
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","020",MIN(c),MAX(c);
FROM taxish20150401_agg2020
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","020",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time025;
CREATE TABLE taxish20150401_time025(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 02:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time025
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 03:00:00';

DROP TABLE IF EXISTS taxish20150401_St025;
CREATE EXTERNAL TABLE taxish20150401_St025(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St025
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time025 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time025;
DROP TABLE IF EXISTS taxish20150401_Stmax025;
CREATE TABLE taxish20150401_Stmax025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin025;
CREATE TABLE taxish20150401_Stmin025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St025 t GROUP BY t.carId) ts,taxish20150401_St025 t
INSERT OVERWRITE TABLE taxish20150401_Stmax025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St025 t GROUP BY t.carId) ts,taxish20150401_St025 t
INSERT OVERWRITE TABLE taxish20150401_Stmin025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp025;
CREATE TABLE taxish20150401_STODp025(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin025 tmin,taxish20150401_Stmax025 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp025
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf025;
CREATE TABLE taxish20150401_STODf025(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf025
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp025 od1 JOIN taxish20150401_STODp025 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD025;
CREATE TABLE taxish20150401_STOD025(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD025
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf025
WHERE count>0;
drop table taxish20150401_Stmax025;
drop table taxish20150401_Stmin025;
drop table taxish20150401_STODf025;
FROM taxish20150401_STOD025
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","025",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO025;
CREATE TABLE taxish20150401_STO025(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD025;
CREATE TABLE taxish20150401_STD025(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO025
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp025
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD025
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp025
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP025;
CREATE TABLE taxish20150401_STTP025(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO025 o,taxish20150401_STD025 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP025
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO025;
drop table taxish20150401_STD025;
drop table taxish20150401_STODp025;
FROM taxish20150401_STTP025
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","025",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg025;
CREATE TABLE taxish20150401_stagg025(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg025
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St025 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg025
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","025",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1025;
CREATE TABLE taxish20150401_agg1025(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St025) bins
INSERT OVERWRITE TABLE taxish20150401_agg1025
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2025;
CREATE TABLE taxish20150401_agg2025(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St025) bins
INSERT OVERWRITE TABLE taxish20150401_agg2025
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1025
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","025",MIN(c),MAX(c);
FROM taxish20150401_agg2025
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","025",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time030;
CREATE TABLE taxish20150401_time030(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 03:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time030
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 03:30:00';

DROP TABLE IF EXISTS taxish20150401_St030;
CREATE EXTERNAL TABLE taxish20150401_St030(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St030
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time030 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time030;
DROP TABLE IF EXISTS taxish20150401_Stmax030;
CREATE TABLE taxish20150401_Stmax030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin030;
CREATE TABLE taxish20150401_Stmin030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St030 t GROUP BY t.carId) ts,taxish20150401_St030 t
INSERT OVERWRITE TABLE taxish20150401_Stmax030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St030 t GROUP BY t.carId) ts,taxish20150401_St030 t
INSERT OVERWRITE TABLE taxish20150401_Stmin030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp030;
CREATE TABLE taxish20150401_STODp030(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin030 tmin,taxish20150401_Stmax030 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp030
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf030;
CREATE TABLE taxish20150401_STODf030(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf030
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp030 od1 JOIN taxish20150401_STODp030 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD030;
CREATE TABLE taxish20150401_STOD030(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD030
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf030
WHERE count>0;
drop table taxish20150401_Stmax030;
drop table taxish20150401_Stmin030;
drop table taxish20150401_STODf030;
FROM taxish20150401_STOD030
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","030",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO030;
CREATE TABLE taxish20150401_STO030(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD030;
CREATE TABLE taxish20150401_STD030(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO030
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp030
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD030
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp030
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP030;
CREATE TABLE taxish20150401_STTP030(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO030 o,taxish20150401_STD030 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP030
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO030;
drop table taxish20150401_STD030;
drop table taxish20150401_STODp030;
FROM taxish20150401_STTP030
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","030",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg030;
CREATE TABLE taxish20150401_stagg030(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg030
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St030 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg030
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","030",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1030;
CREATE TABLE taxish20150401_agg1030(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St030) bins
INSERT OVERWRITE TABLE taxish20150401_agg1030
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2030;
CREATE TABLE taxish20150401_agg2030(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St030) bins
INSERT OVERWRITE TABLE taxish20150401_agg2030
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1030
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","030",MIN(c),MAX(c);
FROM taxish20150401_agg2030
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","030",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time035;
CREATE TABLE taxish20150401_time035(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 03:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time035
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 04:00:00';

DROP TABLE IF EXISTS taxish20150401_St035;
CREATE EXTERNAL TABLE taxish20150401_St035(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St035
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time035 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time035;
DROP TABLE IF EXISTS taxish20150401_Stmax035;
CREATE TABLE taxish20150401_Stmax035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin035;
CREATE TABLE taxish20150401_Stmin035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St035 t GROUP BY t.carId) ts,taxish20150401_St035 t
INSERT OVERWRITE TABLE taxish20150401_Stmax035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St035 t GROUP BY t.carId) ts,taxish20150401_St035 t
INSERT OVERWRITE TABLE taxish20150401_Stmin035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp035;
CREATE TABLE taxish20150401_STODp035(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin035 tmin,taxish20150401_Stmax035 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp035
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf035;
CREATE TABLE taxish20150401_STODf035(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf035
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp035 od1 JOIN taxish20150401_STODp035 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD035;
CREATE TABLE taxish20150401_STOD035(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD035
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf035
WHERE count>0;
drop table taxish20150401_Stmax035;
drop table taxish20150401_Stmin035;
drop table taxish20150401_STODf035;
FROM taxish20150401_STOD035
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","035",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO035;
CREATE TABLE taxish20150401_STO035(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD035;
CREATE TABLE taxish20150401_STD035(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO035
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp035
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD035
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp035
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP035;
CREATE TABLE taxish20150401_STTP035(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO035 o,taxish20150401_STD035 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP035
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO035;
drop table taxish20150401_STD035;
drop table taxish20150401_STODp035;
FROM taxish20150401_STTP035
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","035",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg035;
CREATE TABLE taxish20150401_stagg035(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg035
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St035 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg035
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","035",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1035;
CREATE TABLE taxish20150401_agg1035(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St035) bins
INSERT OVERWRITE TABLE taxish20150401_agg1035
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2035;
CREATE TABLE taxish20150401_agg2035(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St035) bins
INSERT OVERWRITE TABLE taxish20150401_agg2035
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1035
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","035",MIN(c),MAX(c);
FROM taxish20150401_agg2035
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","035",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time040;
CREATE TABLE taxish20150401_time040(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 04:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time040
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 04:30:00';

DROP TABLE IF EXISTS taxish20150401_St040;
CREATE EXTERNAL TABLE taxish20150401_St040(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St040
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time040 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time040;
DROP TABLE IF EXISTS taxish20150401_Stmax040;
CREATE TABLE taxish20150401_Stmax040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin040;
CREATE TABLE taxish20150401_Stmin040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St040 t GROUP BY t.carId) ts,taxish20150401_St040 t
INSERT OVERWRITE TABLE taxish20150401_Stmax040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St040 t GROUP BY t.carId) ts,taxish20150401_St040 t
INSERT OVERWRITE TABLE taxish20150401_Stmin040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp040;
CREATE TABLE taxish20150401_STODp040(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin040 tmin,taxish20150401_Stmax040 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp040
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf040;
CREATE TABLE taxish20150401_STODf040(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf040
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp040 od1 JOIN taxish20150401_STODp040 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD040;
CREATE TABLE taxish20150401_STOD040(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD040
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf040
WHERE count>0;
drop table taxish20150401_Stmax040;
drop table taxish20150401_Stmin040;
drop table taxish20150401_STODf040;
FROM taxish20150401_STOD040
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","040",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO040;
CREATE TABLE taxish20150401_STO040(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD040;
CREATE TABLE taxish20150401_STD040(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO040
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp040
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD040
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp040
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP040;
CREATE TABLE taxish20150401_STTP040(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO040 o,taxish20150401_STD040 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP040
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO040;
drop table taxish20150401_STD040;
drop table taxish20150401_STODp040;
FROM taxish20150401_STTP040
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","040",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg040;
CREATE TABLE taxish20150401_stagg040(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg040
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St040 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg040
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","040",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1040;
CREATE TABLE taxish20150401_agg1040(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St040) bins
INSERT OVERWRITE TABLE taxish20150401_agg1040
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2040;
CREATE TABLE taxish20150401_agg2040(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St040) bins
INSERT OVERWRITE TABLE taxish20150401_agg2040
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1040
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","040",MIN(c),MAX(c);
FROM taxish20150401_agg2040
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","040",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time045;
CREATE TABLE taxish20150401_time045(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 04:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time045
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 05:00:00';

DROP TABLE IF EXISTS taxish20150401_St045;
CREATE EXTERNAL TABLE taxish20150401_St045(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St045
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time045 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time045;
DROP TABLE IF EXISTS taxish20150401_Stmax045;
CREATE TABLE taxish20150401_Stmax045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin045;
CREATE TABLE taxish20150401_Stmin045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St045 t GROUP BY t.carId) ts,taxish20150401_St045 t
INSERT OVERWRITE TABLE taxish20150401_Stmax045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St045 t GROUP BY t.carId) ts,taxish20150401_St045 t
INSERT OVERWRITE TABLE taxish20150401_Stmin045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp045;
CREATE TABLE taxish20150401_STODp045(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin045 tmin,taxish20150401_Stmax045 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp045
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf045;
CREATE TABLE taxish20150401_STODf045(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf045
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp045 od1 JOIN taxish20150401_STODp045 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD045;
CREATE TABLE taxish20150401_STOD045(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD045
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf045
WHERE count>0;
drop table taxish20150401_Stmax045;
drop table taxish20150401_Stmin045;
drop table taxish20150401_STODf045;
FROM taxish20150401_STOD045
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","045",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO045;
CREATE TABLE taxish20150401_STO045(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD045;
CREATE TABLE taxish20150401_STD045(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO045
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp045
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD045
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp045
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP045;
CREATE TABLE taxish20150401_STTP045(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO045 o,taxish20150401_STD045 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP045
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO045;
drop table taxish20150401_STD045;
drop table taxish20150401_STODp045;
FROM taxish20150401_STTP045
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","045",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg045;
CREATE TABLE taxish20150401_stagg045(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg045
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St045 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg045
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","045",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1045;
CREATE TABLE taxish20150401_agg1045(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St045) bins
INSERT OVERWRITE TABLE taxish20150401_agg1045
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2045;
CREATE TABLE taxish20150401_agg2045(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St045) bins
INSERT OVERWRITE TABLE taxish20150401_agg2045
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1045
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","045",MIN(c),MAX(c);
FROM taxish20150401_agg2045
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","045",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time050;
CREATE TABLE taxish20150401_time050(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 05:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time050
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 05:30:00';

DROP TABLE IF EXISTS taxish20150401_St050;
CREATE EXTERNAL TABLE taxish20150401_St050(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St050
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time050 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time050;
DROP TABLE IF EXISTS taxish20150401_Stmax050;
CREATE TABLE taxish20150401_Stmax050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin050;
CREATE TABLE taxish20150401_Stmin050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St050 t GROUP BY t.carId) ts,taxish20150401_St050 t
INSERT OVERWRITE TABLE taxish20150401_Stmax050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St050 t GROUP BY t.carId) ts,taxish20150401_St050 t
INSERT OVERWRITE TABLE taxish20150401_Stmin050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp050;
CREATE TABLE taxish20150401_STODp050(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin050 tmin,taxish20150401_Stmax050 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp050
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf050;
CREATE TABLE taxish20150401_STODf050(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf050
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp050 od1 JOIN taxish20150401_STODp050 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD050;
CREATE TABLE taxish20150401_STOD050(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD050
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf050
WHERE count>0;
drop table taxish20150401_Stmax050;
drop table taxish20150401_Stmin050;
drop table taxish20150401_STODf050;
FROM taxish20150401_STOD050
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","050",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO050;
CREATE TABLE taxish20150401_STO050(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD050;
CREATE TABLE taxish20150401_STD050(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO050
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp050
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD050
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp050
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP050;
CREATE TABLE taxish20150401_STTP050(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO050 o,taxish20150401_STD050 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP050
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO050;
drop table taxish20150401_STD050;
drop table taxish20150401_STODp050;
FROM taxish20150401_STTP050
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","050",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg050;
CREATE TABLE taxish20150401_stagg050(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg050
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St050 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg050
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","050",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1050;
CREATE TABLE taxish20150401_agg1050(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St050) bins
INSERT OVERWRITE TABLE taxish20150401_agg1050
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2050;
CREATE TABLE taxish20150401_agg2050(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St050) bins
INSERT OVERWRITE TABLE taxish20150401_agg2050
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1050
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","050",MIN(c),MAX(c);
FROM taxish20150401_agg2050
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","050",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time055;
CREATE TABLE taxish20150401_time055(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 05:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time055
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 06:00:00';

DROP TABLE IF EXISTS taxish20150401_St055;
CREATE EXTERNAL TABLE taxish20150401_St055(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St055
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time055 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time055;
DROP TABLE IF EXISTS taxish20150401_Stmax055;
CREATE TABLE taxish20150401_Stmax055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin055;
CREATE TABLE taxish20150401_Stmin055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St055 t GROUP BY t.carId) ts,taxish20150401_St055 t
INSERT OVERWRITE TABLE taxish20150401_Stmax055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St055 t GROUP BY t.carId) ts,taxish20150401_St055 t
INSERT OVERWRITE TABLE taxish20150401_Stmin055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp055;
CREATE TABLE taxish20150401_STODp055(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin055 tmin,taxish20150401_Stmax055 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp055
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf055;
CREATE TABLE taxish20150401_STODf055(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf055
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp055 od1 JOIN taxish20150401_STODp055 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD055;
CREATE TABLE taxish20150401_STOD055(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD055
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf055
WHERE count>0;
drop table taxish20150401_Stmax055;
drop table taxish20150401_Stmin055;
drop table taxish20150401_STODf055;
FROM taxish20150401_STOD055
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","055",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO055;
CREATE TABLE taxish20150401_STO055(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD055;
CREATE TABLE taxish20150401_STD055(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO055
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp055
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD055
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp055
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP055;
CREATE TABLE taxish20150401_STTP055(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO055 o,taxish20150401_STD055 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP055
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO055;
drop table taxish20150401_STD055;
drop table taxish20150401_STODp055;
FROM taxish20150401_STTP055
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","055",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg055;
CREATE TABLE taxish20150401_stagg055(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg055
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St055 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg055
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","055",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1055;
CREATE TABLE taxish20150401_agg1055(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St055) bins
INSERT OVERWRITE TABLE taxish20150401_agg1055
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2055;
CREATE TABLE taxish20150401_agg2055(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St055) bins
INSERT OVERWRITE TABLE taxish20150401_agg2055
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1055
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","055",MIN(c),MAX(c);
FROM taxish20150401_agg2055
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","055",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time060;
CREATE TABLE taxish20150401_time060(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 06:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time060
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 06:30:00';

DROP TABLE IF EXISTS taxish20150401_St060;
CREATE EXTERNAL TABLE taxish20150401_St060(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St060
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time060 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time060;
DROP TABLE IF EXISTS taxish20150401_Stmax060;
CREATE TABLE taxish20150401_Stmax060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin060;
CREATE TABLE taxish20150401_Stmin060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St060 t GROUP BY t.carId) ts,taxish20150401_St060 t
INSERT OVERWRITE TABLE taxish20150401_Stmax060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St060 t GROUP BY t.carId) ts,taxish20150401_St060 t
INSERT OVERWRITE TABLE taxish20150401_Stmin060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp060;
CREATE TABLE taxish20150401_STODp060(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin060 tmin,taxish20150401_Stmax060 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp060
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf060;
CREATE TABLE taxish20150401_STODf060(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf060
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp060 od1 JOIN taxish20150401_STODp060 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD060;
CREATE TABLE taxish20150401_STOD060(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD060
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf060
WHERE count>0;
drop table taxish20150401_Stmax060;
drop table taxish20150401_Stmin060;
drop table taxish20150401_STODf060;
FROM taxish20150401_STOD060
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","060",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO060;
CREATE TABLE taxish20150401_STO060(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD060;
CREATE TABLE taxish20150401_STD060(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO060
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp060
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD060
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp060
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP060;
CREATE TABLE taxish20150401_STTP060(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO060 o,taxish20150401_STD060 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP060
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO060;
drop table taxish20150401_STD060;
drop table taxish20150401_STODp060;
FROM taxish20150401_STTP060
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","060",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg060;
CREATE TABLE taxish20150401_stagg060(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg060
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St060 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg060
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","060",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1060;
CREATE TABLE taxish20150401_agg1060(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St060) bins
INSERT OVERWRITE TABLE taxish20150401_agg1060
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2060;
CREATE TABLE taxish20150401_agg2060(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St060) bins
INSERT OVERWRITE TABLE taxish20150401_agg2060
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1060
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","060",MIN(c),MAX(c);
FROM taxish20150401_agg2060
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","060",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time065;
CREATE TABLE taxish20150401_time065(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 06:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time065
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 07:00:00';

DROP TABLE IF EXISTS taxish20150401_St065;
CREATE EXTERNAL TABLE taxish20150401_St065(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St065
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time065 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time065;
DROP TABLE IF EXISTS taxish20150401_Stmax065;
CREATE TABLE taxish20150401_Stmax065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin065;
CREATE TABLE taxish20150401_Stmin065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St065 t GROUP BY t.carId) ts,taxish20150401_St065 t
INSERT OVERWRITE TABLE taxish20150401_Stmax065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St065 t GROUP BY t.carId) ts,taxish20150401_St065 t
INSERT OVERWRITE TABLE taxish20150401_Stmin065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp065;
CREATE TABLE taxish20150401_STODp065(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin065 tmin,taxish20150401_Stmax065 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp065
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf065;
CREATE TABLE taxish20150401_STODf065(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf065
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp065 od1 JOIN taxish20150401_STODp065 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD065;
CREATE TABLE taxish20150401_STOD065(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD065
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf065
WHERE count>0;
drop table taxish20150401_Stmax065;
drop table taxish20150401_Stmin065;
drop table taxish20150401_STODf065;
FROM taxish20150401_STOD065
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","065",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO065;
CREATE TABLE taxish20150401_STO065(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD065;
CREATE TABLE taxish20150401_STD065(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO065
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp065
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD065
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp065
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP065;
CREATE TABLE taxish20150401_STTP065(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO065 o,taxish20150401_STD065 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP065
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO065;
drop table taxish20150401_STD065;
drop table taxish20150401_STODp065;
FROM taxish20150401_STTP065
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","065",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg065;
CREATE TABLE taxish20150401_stagg065(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg065
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St065 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg065
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","065",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1065;
CREATE TABLE taxish20150401_agg1065(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St065) bins
INSERT OVERWRITE TABLE taxish20150401_agg1065
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2065;
CREATE TABLE taxish20150401_agg2065(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St065) bins
INSERT OVERWRITE TABLE taxish20150401_agg2065
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1065
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","065",MIN(c),MAX(c);
FROM taxish20150401_agg2065
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","065",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time070;
CREATE TABLE taxish20150401_time070(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 07:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time070
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 07:30:00';

DROP TABLE IF EXISTS taxish20150401_St070;
CREATE EXTERNAL TABLE taxish20150401_St070(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St070
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time070 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time070;
DROP TABLE IF EXISTS taxish20150401_Stmax070;
CREATE TABLE taxish20150401_Stmax070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin070;
CREATE TABLE taxish20150401_Stmin070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St070 t GROUP BY t.carId) ts,taxish20150401_St070 t
INSERT OVERWRITE TABLE taxish20150401_Stmax070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St070 t GROUP BY t.carId) ts,taxish20150401_St070 t
INSERT OVERWRITE TABLE taxish20150401_Stmin070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp070;
CREATE TABLE taxish20150401_STODp070(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin070 tmin,taxish20150401_Stmax070 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp070
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf070;
CREATE TABLE taxish20150401_STODf070(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf070
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp070 od1 JOIN taxish20150401_STODp070 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD070;
CREATE TABLE taxish20150401_STOD070(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD070
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf070
WHERE count>0;
drop table taxish20150401_Stmax070;
drop table taxish20150401_Stmin070;
drop table taxish20150401_STODf070;
FROM taxish20150401_STOD070
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","070",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO070;
CREATE TABLE taxish20150401_STO070(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD070;
CREATE TABLE taxish20150401_STD070(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO070
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp070
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD070
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp070
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP070;
CREATE TABLE taxish20150401_STTP070(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO070 o,taxish20150401_STD070 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP070
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO070;
drop table taxish20150401_STD070;
drop table taxish20150401_STODp070;
FROM taxish20150401_STTP070
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","070",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg070;
CREATE TABLE taxish20150401_stagg070(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg070
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St070 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg070
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","070",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1070;
CREATE TABLE taxish20150401_agg1070(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St070) bins
INSERT OVERWRITE TABLE taxish20150401_agg1070
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2070;
CREATE TABLE taxish20150401_agg2070(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St070) bins
INSERT OVERWRITE TABLE taxish20150401_agg2070
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1070
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","070",MIN(c),MAX(c);
FROM taxish20150401_agg2070
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","070",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time075;
CREATE TABLE taxish20150401_time075(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 07:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time075
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 08:00:00';

DROP TABLE IF EXISTS taxish20150401_St075;
CREATE EXTERNAL TABLE taxish20150401_St075(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St075
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time075 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time075;
DROP TABLE IF EXISTS taxish20150401_Stmax075;
CREATE TABLE taxish20150401_Stmax075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin075;
CREATE TABLE taxish20150401_Stmin075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St075 t GROUP BY t.carId) ts,taxish20150401_St075 t
INSERT OVERWRITE TABLE taxish20150401_Stmax075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St075 t GROUP BY t.carId) ts,taxish20150401_St075 t
INSERT OVERWRITE TABLE taxish20150401_Stmin075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp075;
CREATE TABLE taxish20150401_STODp075(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin075 tmin,taxish20150401_Stmax075 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp075
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf075;
CREATE TABLE taxish20150401_STODf075(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf075
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp075 od1 JOIN taxish20150401_STODp075 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD075;
CREATE TABLE taxish20150401_STOD075(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD075
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf075
WHERE count>0;
drop table taxish20150401_Stmax075;
drop table taxish20150401_Stmin075;
drop table taxish20150401_STODf075;
FROM taxish20150401_STOD075
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","075",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO075;
CREATE TABLE taxish20150401_STO075(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD075;
CREATE TABLE taxish20150401_STD075(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO075
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp075
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD075
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp075
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP075;
CREATE TABLE taxish20150401_STTP075(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO075 o,taxish20150401_STD075 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP075
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO075;
drop table taxish20150401_STD075;
drop table taxish20150401_STODp075;
FROM taxish20150401_STTP075
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","075",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg075;
CREATE TABLE taxish20150401_stagg075(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg075
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St075 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg075
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","075",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1075;
CREATE TABLE taxish20150401_agg1075(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St075) bins
INSERT OVERWRITE TABLE taxish20150401_agg1075
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2075;
CREATE TABLE taxish20150401_agg2075(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St075) bins
INSERT OVERWRITE TABLE taxish20150401_agg2075
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1075
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","075",MIN(c),MAX(c);
FROM taxish20150401_agg2075
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","075",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time080;
CREATE TABLE taxish20150401_time080(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 08:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time080
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 08:30:00';

DROP TABLE IF EXISTS taxish20150401_St080;
CREATE EXTERNAL TABLE taxish20150401_St080(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St080
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time080 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time080;
DROP TABLE IF EXISTS taxish20150401_Stmax080;
CREATE TABLE taxish20150401_Stmax080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin080;
CREATE TABLE taxish20150401_Stmin080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St080 t GROUP BY t.carId) ts,taxish20150401_St080 t
INSERT OVERWRITE TABLE taxish20150401_Stmax080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St080 t GROUP BY t.carId) ts,taxish20150401_St080 t
INSERT OVERWRITE TABLE taxish20150401_Stmin080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp080;
CREATE TABLE taxish20150401_STODp080(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin080 tmin,taxish20150401_Stmax080 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp080
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf080;
CREATE TABLE taxish20150401_STODf080(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf080
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp080 od1 JOIN taxish20150401_STODp080 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD080;
CREATE TABLE taxish20150401_STOD080(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD080
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf080
WHERE count>0;
drop table taxish20150401_Stmax080;
drop table taxish20150401_Stmin080;
drop table taxish20150401_STODf080;
FROM taxish20150401_STOD080
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","080",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO080;
CREATE TABLE taxish20150401_STO080(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD080;
CREATE TABLE taxish20150401_STD080(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO080
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp080
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD080
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp080
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP080;
CREATE TABLE taxish20150401_STTP080(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO080 o,taxish20150401_STD080 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP080
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO080;
drop table taxish20150401_STD080;
drop table taxish20150401_STODp080;
FROM taxish20150401_STTP080
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","080",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg080;
CREATE TABLE taxish20150401_stagg080(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg080
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St080 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg080
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","080",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1080;
CREATE TABLE taxish20150401_agg1080(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St080) bins
INSERT OVERWRITE TABLE taxish20150401_agg1080
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2080;
CREATE TABLE taxish20150401_agg2080(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St080) bins
INSERT OVERWRITE TABLE taxish20150401_agg2080
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1080
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","080",MIN(c),MAX(c);
FROM taxish20150401_agg2080
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","080",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time085;
CREATE TABLE taxish20150401_time085(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 08:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time085
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 09:00:00';

DROP TABLE IF EXISTS taxish20150401_St085;
CREATE EXTERNAL TABLE taxish20150401_St085(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St085
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time085 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time085;
DROP TABLE IF EXISTS taxish20150401_Stmax085;
CREATE TABLE taxish20150401_Stmax085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin085;
CREATE TABLE taxish20150401_Stmin085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St085 t GROUP BY t.carId) ts,taxish20150401_St085 t
INSERT OVERWRITE TABLE taxish20150401_Stmax085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St085 t GROUP BY t.carId) ts,taxish20150401_St085 t
INSERT OVERWRITE TABLE taxish20150401_Stmin085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp085;
CREATE TABLE taxish20150401_STODp085(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin085 tmin,taxish20150401_Stmax085 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp085
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf085;
CREATE TABLE taxish20150401_STODf085(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf085
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp085 od1 JOIN taxish20150401_STODp085 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD085;
CREATE TABLE taxish20150401_STOD085(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD085
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf085
WHERE count>0;
drop table taxish20150401_Stmax085;
drop table taxish20150401_Stmin085;
drop table taxish20150401_STODf085;
FROM taxish20150401_STOD085
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","085",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO085;
CREATE TABLE taxish20150401_STO085(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD085;
CREATE TABLE taxish20150401_STD085(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO085
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp085
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD085
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp085
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP085;
CREATE TABLE taxish20150401_STTP085(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO085 o,taxish20150401_STD085 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP085
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO085;
drop table taxish20150401_STD085;
drop table taxish20150401_STODp085;
FROM taxish20150401_STTP085
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","085",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg085;
CREATE TABLE taxish20150401_stagg085(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg085
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St085 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg085
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","085",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1085;
CREATE TABLE taxish20150401_agg1085(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St085) bins
INSERT OVERWRITE TABLE taxish20150401_agg1085
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2085;
CREATE TABLE taxish20150401_agg2085(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St085) bins
INSERT OVERWRITE TABLE taxish20150401_agg2085
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1085
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","085",MIN(c),MAX(c);
FROM taxish20150401_agg2085
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","085",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time090;
CREATE TABLE taxish20150401_time090(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 09:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time090
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 09:30:00';

DROP TABLE IF EXISTS taxish20150401_St090;
CREATE EXTERNAL TABLE taxish20150401_St090(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St090
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time090 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time090;
DROP TABLE IF EXISTS taxish20150401_Stmax090;
CREATE TABLE taxish20150401_Stmax090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin090;
CREATE TABLE taxish20150401_Stmin090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St090 t GROUP BY t.carId) ts,taxish20150401_St090 t
INSERT OVERWRITE TABLE taxish20150401_Stmax090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St090 t GROUP BY t.carId) ts,taxish20150401_St090 t
INSERT OVERWRITE TABLE taxish20150401_Stmin090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp090;
CREATE TABLE taxish20150401_STODp090(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin090 tmin,taxish20150401_Stmax090 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp090
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf090;
CREATE TABLE taxish20150401_STODf090(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf090
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp090 od1 JOIN taxish20150401_STODp090 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD090;
CREATE TABLE taxish20150401_STOD090(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD090
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf090
WHERE count>0;
drop table taxish20150401_Stmax090;
drop table taxish20150401_Stmin090;
drop table taxish20150401_STODf090;
FROM taxish20150401_STOD090
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","090",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO090;
CREATE TABLE taxish20150401_STO090(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD090;
CREATE TABLE taxish20150401_STD090(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO090
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp090
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD090
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp090
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP090;
CREATE TABLE taxish20150401_STTP090(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO090 o,taxish20150401_STD090 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP090
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO090;
drop table taxish20150401_STD090;
drop table taxish20150401_STODp090;
FROM taxish20150401_STTP090
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","090",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg090;
CREATE TABLE taxish20150401_stagg090(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg090
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St090 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg090
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","090",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1090;
CREATE TABLE taxish20150401_agg1090(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St090) bins
INSERT OVERWRITE TABLE taxish20150401_agg1090
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2090;
CREATE TABLE taxish20150401_agg2090(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St090) bins
INSERT OVERWRITE TABLE taxish20150401_agg2090
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1090
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","090",MIN(c),MAX(c);
FROM taxish20150401_agg2090
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","090",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time095;
CREATE TABLE taxish20150401_time095(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 09:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time095
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 10:00:00';

DROP TABLE IF EXISTS taxish20150401_St095;
CREATE EXTERNAL TABLE taxish20150401_St095(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St095
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time095 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time095;
DROP TABLE IF EXISTS taxish20150401_Stmax095;
CREATE TABLE taxish20150401_Stmax095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin095;
CREATE TABLE taxish20150401_Stmin095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St095 t GROUP BY t.carId) ts,taxish20150401_St095 t
INSERT OVERWRITE TABLE taxish20150401_Stmax095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St095 t GROUP BY t.carId) ts,taxish20150401_St095 t
INSERT OVERWRITE TABLE taxish20150401_Stmin095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp095;
CREATE TABLE taxish20150401_STODp095(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin095 tmin,taxish20150401_Stmax095 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp095
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf095;
CREATE TABLE taxish20150401_STODf095(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf095
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp095 od1 JOIN taxish20150401_STODp095 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD095;
CREATE TABLE taxish20150401_STOD095(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD095
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf095
WHERE count>0;
drop table taxish20150401_Stmax095;
drop table taxish20150401_Stmin095;
drop table taxish20150401_STODf095;
FROM taxish20150401_STOD095
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","095",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO095;
CREATE TABLE taxish20150401_STO095(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD095;
CREATE TABLE taxish20150401_STD095(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO095
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp095
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD095
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp095
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP095;
CREATE TABLE taxish20150401_STTP095(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO095 o,taxish20150401_STD095 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP095
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO095;
drop table taxish20150401_STD095;
drop table taxish20150401_STODp095;
FROM taxish20150401_STTP095
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","095",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg095;
CREATE TABLE taxish20150401_stagg095(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg095
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St095 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg095
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","095",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1095;
CREATE TABLE taxish20150401_agg1095(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St095) bins
INSERT OVERWRITE TABLE taxish20150401_agg1095
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2095;
CREATE TABLE taxish20150401_agg2095(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St095) bins
INSERT OVERWRITE TABLE taxish20150401_agg2095
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1095
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","095",MIN(c),MAX(c);
FROM taxish20150401_agg2095
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","095",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time100;
CREATE TABLE taxish20150401_time100(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 10:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time100
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 10:30:00';

DROP TABLE IF EXISTS taxish20150401_St100;
CREATE EXTERNAL TABLE taxish20150401_St100(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St100
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time100 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time100;
DROP TABLE IF EXISTS taxish20150401_Stmax100;
CREATE TABLE taxish20150401_Stmax100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin100;
CREATE TABLE taxish20150401_Stmin100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St100 t GROUP BY t.carId) ts,taxish20150401_St100 t
INSERT OVERWRITE TABLE taxish20150401_Stmax100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St100 t GROUP BY t.carId) ts,taxish20150401_St100 t
INSERT OVERWRITE TABLE taxish20150401_Stmin100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp100;
CREATE TABLE taxish20150401_STODp100(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin100 tmin,taxish20150401_Stmax100 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp100
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf100;
CREATE TABLE taxish20150401_STODf100(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf100
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp100 od1 JOIN taxish20150401_STODp100 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD100;
CREATE TABLE taxish20150401_STOD100(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD100
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf100
WHERE count>0;
drop table taxish20150401_Stmax100;
drop table taxish20150401_Stmin100;
drop table taxish20150401_STODf100;
FROM taxish20150401_STOD100
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","100",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO100;
CREATE TABLE taxish20150401_STO100(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD100;
CREATE TABLE taxish20150401_STD100(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO100
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp100
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD100
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp100
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP100;
CREATE TABLE taxish20150401_STTP100(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO100 o,taxish20150401_STD100 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP100
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO100;
drop table taxish20150401_STD100;
drop table taxish20150401_STODp100;
FROM taxish20150401_STTP100
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","100",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg100;
CREATE TABLE taxish20150401_stagg100(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg100
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St100 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg100
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","100",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1100;
CREATE TABLE taxish20150401_agg1100(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St100) bins
INSERT OVERWRITE TABLE taxish20150401_agg1100
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2100;
CREATE TABLE taxish20150401_agg2100(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St100) bins
INSERT OVERWRITE TABLE taxish20150401_agg2100
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1100
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","100",MIN(c),MAX(c);
FROM taxish20150401_agg2100
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","100",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time105;
CREATE TABLE taxish20150401_time105(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 10:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time105
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 11:00:00';

DROP TABLE IF EXISTS taxish20150401_St105;
CREATE EXTERNAL TABLE taxish20150401_St105(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St105
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time105 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time105;
DROP TABLE IF EXISTS taxish20150401_Stmax105;
CREATE TABLE taxish20150401_Stmax105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin105;
CREATE TABLE taxish20150401_Stmin105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St105 t GROUP BY t.carId) ts,taxish20150401_St105 t
INSERT OVERWRITE TABLE taxish20150401_Stmax105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St105 t GROUP BY t.carId) ts,taxish20150401_St105 t
INSERT OVERWRITE TABLE taxish20150401_Stmin105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp105;
CREATE TABLE taxish20150401_STODp105(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin105 tmin,taxish20150401_Stmax105 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp105
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf105;
CREATE TABLE taxish20150401_STODf105(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf105
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp105 od1 JOIN taxish20150401_STODp105 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD105;
CREATE TABLE taxish20150401_STOD105(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD105
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf105
WHERE count>0;
drop table taxish20150401_Stmax105;
drop table taxish20150401_Stmin105;
drop table taxish20150401_STODf105;
FROM taxish20150401_STOD105
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","105",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO105;
CREATE TABLE taxish20150401_STO105(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD105;
CREATE TABLE taxish20150401_STD105(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO105
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp105
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD105
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp105
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP105;
CREATE TABLE taxish20150401_STTP105(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO105 o,taxish20150401_STD105 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP105
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO105;
drop table taxish20150401_STD105;
drop table taxish20150401_STODp105;
FROM taxish20150401_STTP105
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","105",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg105;
CREATE TABLE taxish20150401_stagg105(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg105
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St105 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg105
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","105",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1105;
CREATE TABLE taxish20150401_agg1105(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St105) bins
INSERT OVERWRITE TABLE taxish20150401_agg1105
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2105;
CREATE TABLE taxish20150401_agg2105(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St105) bins
INSERT OVERWRITE TABLE taxish20150401_agg2105
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1105
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","105",MIN(c),MAX(c);
FROM taxish20150401_agg2105
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","105",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time110;
CREATE TABLE taxish20150401_time110(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 11:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time110
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 11:30:00';

DROP TABLE IF EXISTS taxish20150401_St110;
CREATE EXTERNAL TABLE taxish20150401_St110(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St110
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time110 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time110;
DROP TABLE IF EXISTS taxish20150401_Stmax110;
CREATE TABLE taxish20150401_Stmax110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin110;
CREATE TABLE taxish20150401_Stmin110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St110 t GROUP BY t.carId) ts,taxish20150401_St110 t
INSERT OVERWRITE TABLE taxish20150401_Stmax110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St110 t GROUP BY t.carId) ts,taxish20150401_St110 t
INSERT OVERWRITE TABLE taxish20150401_Stmin110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp110;
CREATE TABLE taxish20150401_STODp110(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin110 tmin,taxish20150401_Stmax110 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp110
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf110;
CREATE TABLE taxish20150401_STODf110(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf110
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp110 od1 JOIN taxish20150401_STODp110 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD110;
CREATE TABLE taxish20150401_STOD110(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD110
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf110
WHERE count>0;
drop table taxish20150401_Stmax110;
drop table taxish20150401_Stmin110;
drop table taxish20150401_STODf110;
FROM taxish20150401_STOD110
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","110",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO110;
CREATE TABLE taxish20150401_STO110(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD110;
CREATE TABLE taxish20150401_STD110(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO110
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp110
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD110
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp110
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP110;
CREATE TABLE taxish20150401_STTP110(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO110 o,taxish20150401_STD110 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP110
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO110;
drop table taxish20150401_STD110;
drop table taxish20150401_STODp110;
FROM taxish20150401_STTP110
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","110",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg110;
CREATE TABLE taxish20150401_stagg110(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg110
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St110 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg110
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","110",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1110;
CREATE TABLE taxish20150401_agg1110(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St110) bins
INSERT OVERWRITE TABLE taxish20150401_agg1110
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2110;
CREATE TABLE taxish20150401_agg2110(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St110) bins
INSERT OVERWRITE TABLE taxish20150401_agg2110
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1110
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","110",MIN(c),MAX(c);
FROM taxish20150401_agg2110
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","110",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time115;
CREATE TABLE taxish20150401_time115(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 11:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time115
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 12:00:00';

DROP TABLE IF EXISTS taxish20150401_St115;
CREATE EXTERNAL TABLE taxish20150401_St115(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St115
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time115 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time115;
DROP TABLE IF EXISTS taxish20150401_Stmax115;
CREATE TABLE taxish20150401_Stmax115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin115;
CREATE TABLE taxish20150401_Stmin115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St115 t GROUP BY t.carId) ts,taxish20150401_St115 t
INSERT OVERWRITE TABLE taxish20150401_Stmax115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St115 t GROUP BY t.carId) ts,taxish20150401_St115 t
INSERT OVERWRITE TABLE taxish20150401_Stmin115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp115;
CREATE TABLE taxish20150401_STODp115(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin115 tmin,taxish20150401_Stmax115 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp115
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf115;
CREATE TABLE taxish20150401_STODf115(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf115
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp115 od1 JOIN taxish20150401_STODp115 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD115;
CREATE TABLE taxish20150401_STOD115(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD115
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf115
WHERE count>0;
drop table taxish20150401_Stmax115;
drop table taxish20150401_Stmin115;
drop table taxish20150401_STODf115;
FROM taxish20150401_STOD115
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","115",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO115;
CREATE TABLE taxish20150401_STO115(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD115;
CREATE TABLE taxish20150401_STD115(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO115
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp115
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD115
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp115
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP115;
CREATE TABLE taxish20150401_STTP115(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO115 o,taxish20150401_STD115 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP115
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO115;
drop table taxish20150401_STD115;
drop table taxish20150401_STODp115;
FROM taxish20150401_STTP115
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","115",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg115;
CREATE TABLE taxish20150401_stagg115(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg115
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St115 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg115
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","115",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1115;
CREATE TABLE taxish20150401_agg1115(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St115) bins
INSERT OVERWRITE TABLE taxish20150401_agg1115
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2115;
CREATE TABLE taxish20150401_agg2115(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St115) bins
INSERT OVERWRITE TABLE taxish20150401_agg2115
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1115
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","115",MIN(c),MAX(c);
FROM taxish20150401_agg2115
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","115",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time120;
CREATE TABLE taxish20150401_time120(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 12:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time120
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 12:30:00';

DROP TABLE IF EXISTS taxish20150401_St120;
CREATE EXTERNAL TABLE taxish20150401_St120(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St120
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time120 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time120;
DROP TABLE IF EXISTS taxish20150401_Stmax120;
CREATE TABLE taxish20150401_Stmax120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin120;
CREATE TABLE taxish20150401_Stmin120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St120 t GROUP BY t.carId) ts,taxish20150401_St120 t
INSERT OVERWRITE TABLE taxish20150401_Stmax120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St120 t GROUP BY t.carId) ts,taxish20150401_St120 t
INSERT OVERWRITE TABLE taxish20150401_Stmin120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp120;
CREATE TABLE taxish20150401_STODp120(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin120 tmin,taxish20150401_Stmax120 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp120
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf120;
CREATE TABLE taxish20150401_STODf120(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf120
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp120 od1 JOIN taxish20150401_STODp120 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD120;
CREATE TABLE taxish20150401_STOD120(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD120
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf120
WHERE count>0;
drop table taxish20150401_Stmax120;
drop table taxish20150401_Stmin120;
drop table taxish20150401_STODf120;
FROM taxish20150401_STOD120
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","120",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO120;
CREATE TABLE taxish20150401_STO120(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD120;
CREATE TABLE taxish20150401_STD120(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO120
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp120
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD120
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp120
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP120;
CREATE TABLE taxish20150401_STTP120(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO120 o,taxish20150401_STD120 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP120
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO120;
drop table taxish20150401_STD120;
drop table taxish20150401_STODp120;
FROM taxish20150401_STTP120
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","120",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg120;
CREATE TABLE taxish20150401_stagg120(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg120
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St120 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg120
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","120",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1120;
CREATE TABLE taxish20150401_agg1120(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St120) bins
INSERT OVERWRITE TABLE taxish20150401_agg1120
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2120;
CREATE TABLE taxish20150401_agg2120(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St120) bins
INSERT OVERWRITE TABLE taxish20150401_agg2120
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1120
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","120",MIN(c),MAX(c);
FROM taxish20150401_agg2120
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","120",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time125;
CREATE TABLE taxish20150401_time125(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 12:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time125
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 13:00:00';

DROP TABLE IF EXISTS taxish20150401_St125;
CREATE EXTERNAL TABLE taxish20150401_St125(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St125
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time125 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time125;
DROP TABLE IF EXISTS taxish20150401_Stmax125;
CREATE TABLE taxish20150401_Stmax125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin125;
CREATE TABLE taxish20150401_Stmin125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St125 t GROUP BY t.carId) ts,taxish20150401_St125 t
INSERT OVERWRITE TABLE taxish20150401_Stmax125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St125 t GROUP BY t.carId) ts,taxish20150401_St125 t
INSERT OVERWRITE TABLE taxish20150401_Stmin125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp125;
CREATE TABLE taxish20150401_STODp125(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin125 tmin,taxish20150401_Stmax125 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp125
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf125;
CREATE TABLE taxish20150401_STODf125(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf125
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp125 od1 JOIN taxish20150401_STODp125 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD125;
CREATE TABLE taxish20150401_STOD125(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD125
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf125
WHERE count>0;
drop table taxish20150401_Stmax125;
drop table taxish20150401_Stmin125;
drop table taxish20150401_STODf125;
FROM taxish20150401_STOD125
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","125",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO125;
CREATE TABLE taxish20150401_STO125(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD125;
CREATE TABLE taxish20150401_STD125(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO125
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp125
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD125
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp125
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP125;
CREATE TABLE taxish20150401_STTP125(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO125 o,taxish20150401_STD125 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP125
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO125;
drop table taxish20150401_STD125;
drop table taxish20150401_STODp125;
FROM taxish20150401_STTP125
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","125",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg125;
CREATE TABLE taxish20150401_stagg125(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg125
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St125 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg125
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","125",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1125;
CREATE TABLE taxish20150401_agg1125(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St125) bins
INSERT OVERWRITE TABLE taxish20150401_agg1125
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2125;
CREATE TABLE taxish20150401_agg2125(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St125) bins
INSERT OVERWRITE TABLE taxish20150401_agg2125
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1125
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","125",MIN(c),MAX(c);
FROM taxish20150401_agg2125
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","125",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time130;
CREATE TABLE taxish20150401_time130(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 13:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time130
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 13:30:00';

DROP TABLE IF EXISTS taxish20150401_St130;
CREATE EXTERNAL TABLE taxish20150401_St130(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St130
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time130 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time130;
DROP TABLE IF EXISTS taxish20150401_Stmax130;
CREATE TABLE taxish20150401_Stmax130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin130;
CREATE TABLE taxish20150401_Stmin130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St130 t GROUP BY t.carId) ts,taxish20150401_St130 t
INSERT OVERWRITE TABLE taxish20150401_Stmax130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St130 t GROUP BY t.carId) ts,taxish20150401_St130 t
INSERT OVERWRITE TABLE taxish20150401_Stmin130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp130;
CREATE TABLE taxish20150401_STODp130(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin130 tmin,taxish20150401_Stmax130 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp130
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf130;
CREATE TABLE taxish20150401_STODf130(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf130
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp130 od1 JOIN taxish20150401_STODp130 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD130;
CREATE TABLE taxish20150401_STOD130(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD130
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf130
WHERE count>0;
drop table taxish20150401_Stmax130;
drop table taxish20150401_Stmin130;
drop table taxish20150401_STODf130;
FROM taxish20150401_STOD130
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","130",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO130;
CREATE TABLE taxish20150401_STO130(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD130;
CREATE TABLE taxish20150401_STD130(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO130
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp130
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD130
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp130
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP130;
CREATE TABLE taxish20150401_STTP130(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO130 o,taxish20150401_STD130 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP130
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO130;
drop table taxish20150401_STD130;
drop table taxish20150401_STODp130;
FROM taxish20150401_STTP130
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","130",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg130;
CREATE TABLE taxish20150401_stagg130(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg130
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St130 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg130
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","130",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1130;
CREATE TABLE taxish20150401_agg1130(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St130) bins
INSERT OVERWRITE TABLE taxish20150401_agg1130
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2130;
CREATE TABLE taxish20150401_agg2130(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St130) bins
INSERT OVERWRITE TABLE taxish20150401_agg2130
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1130
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","130",MIN(c),MAX(c);
FROM taxish20150401_agg2130
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","130",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time135;
CREATE TABLE taxish20150401_time135(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 13:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time135
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 14:00:00';

DROP TABLE IF EXISTS taxish20150401_St135;
CREATE EXTERNAL TABLE taxish20150401_St135(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St135
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time135 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time135;
DROP TABLE IF EXISTS taxish20150401_Stmax135;
CREATE TABLE taxish20150401_Stmax135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin135;
CREATE TABLE taxish20150401_Stmin135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St135 t GROUP BY t.carId) ts,taxish20150401_St135 t
INSERT OVERWRITE TABLE taxish20150401_Stmax135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St135 t GROUP BY t.carId) ts,taxish20150401_St135 t
INSERT OVERWRITE TABLE taxish20150401_Stmin135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp135;
CREATE TABLE taxish20150401_STODp135(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin135 tmin,taxish20150401_Stmax135 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp135
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf135;
CREATE TABLE taxish20150401_STODf135(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf135
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp135 od1 JOIN taxish20150401_STODp135 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD135;
CREATE TABLE taxish20150401_STOD135(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD135
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf135
WHERE count>0;
drop table taxish20150401_Stmax135;
drop table taxish20150401_Stmin135;
drop table taxish20150401_STODf135;
FROM taxish20150401_STOD135
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","135",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO135;
CREATE TABLE taxish20150401_STO135(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD135;
CREATE TABLE taxish20150401_STD135(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO135
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp135
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD135
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp135
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP135;
CREATE TABLE taxish20150401_STTP135(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO135 o,taxish20150401_STD135 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP135
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO135;
drop table taxish20150401_STD135;
drop table taxish20150401_STODp135;
FROM taxish20150401_STTP135
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","135",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg135;
CREATE TABLE taxish20150401_stagg135(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg135
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St135 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg135
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","135",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1135;
CREATE TABLE taxish20150401_agg1135(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St135) bins
INSERT OVERWRITE TABLE taxish20150401_agg1135
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2135;
CREATE TABLE taxish20150401_agg2135(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St135) bins
INSERT OVERWRITE TABLE taxish20150401_agg2135
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1135
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","135",MIN(c),MAX(c);
FROM taxish20150401_agg2135
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","135",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time140;
CREATE TABLE taxish20150401_time140(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 14:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time140
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 14:30:00';

DROP TABLE IF EXISTS taxish20150401_St140;
CREATE EXTERNAL TABLE taxish20150401_St140(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St140
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time140 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time140;
DROP TABLE IF EXISTS taxish20150401_Stmax140;
CREATE TABLE taxish20150401_Stmax140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin140;
CREATE TABLE taxish20150401_Stmin140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St140 t GROUP BY t.carId) ts,taxish20150401_St140 t
INSERT OVERWRITE TABLE taxish20150401_Stmax140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St140 t GROUP BY t.carId) ts,taxish20150401_St140 t
INSERT OVERWRITE TABLE taxish20150401_Stmin140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp140;
CREATE TABLE taxish20150401_STODp140(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin140 tmin,taxish20150401_Stmax140 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp140
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf140;
CREATE TABLE taxish20150401_STODf140(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf140
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp140 od1 JOIN taxish20150401_STODp140 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD140;
CREATE TABLE taxish20150401_STOD140(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD140
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf140
WHERE count>0;
drop table taxish20150401_Stmax140;
drop table taxish20150401_Stmin140;
drop table taxish20150401_STODf140;
FROM taxish20150401_STOD140
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","140",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO140;
CREATE TABLE taxish20150401_STO140(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD140;
CREATE TABLE taxish20150401_STD140(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO140
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp140
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD140
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp140
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP140;
CREATE TABLE taxish20150401_STTP140(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO140 o,taxish20150401_STD140 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP140
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO140;
drop table taxish20150401_STD140;
drop table taxish20150401_STODp140;
FROM taxish20150401_STTP140
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","140",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg140;
CREATE TABLE taxish20150401_stagg140(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg140
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St140 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg140
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","140",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1140;
CREATE TABLE taxish20150401_agg1140(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St140) bins
INSERT OVERWRITE TABLE taxish20150401_agg1140
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2140;
CREATE TABLE taxish20150401_agg2140(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St140) bins
INSERT OVERWRITE TABLE taxish20150401_agg2140
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1140
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","140",MIN(c),MAX(c);
FROM taxish20150401_agg2140
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","140",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time145;
CREATE TABLE taxish20150401_time145(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 14:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time145
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 15:00:00';

DROP TABLE IF EXISTS taxish20150401_St145;
CREATE EXTERNAL TABLE taxish20150401_St145(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St145
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time145 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time145;
DROP TABLE IF EXISTS taxish20150401_Stmax145;
CREATE TABLE taxish20150401_Stmax145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin145;
CREATE TABLE taxish20150401_Stmin145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St145 t GROUP BY t.carId) ts,taxish20150401_St145 t
INSERT OVERWRITE TABLE taxish20150401_Stmax145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St145 t GROUP BY t.carId) ts,taxish20150401_St145 t
INSERT OVERWRITE TABLE taxish20150401_Stmin145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp145;
CREATE TABLE taxish20150401_STODp145(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin145 tmin,taxish20150401_Stmax145 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp145
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf145;
CREATE TABLE taxish20150401_STODf145(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf145
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp145 od1 JOIN taxish20150401_STODp145 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD145;
CREATE TABLE taxish20150401_STOD145(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD145
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf145
WHERE count>0;
drop table taxish20150401_Stmax145;
drop table taxish20150401_Stmin145;
drop table taxish20150401_STODf145;
FROM taxish20150401_STOD145
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","145",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO145;
CREATE TABLE taxish20150401_STO145(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD145;
CREATE TABLE taxish20150401_STD145(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO145
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp145
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD145
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp145
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP145;
CREATE TABLE taxish20150401_STTP145(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO145 o,taxish20150401_STD145 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP145
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO145;
drop table taxish20150401_STD145;
drop table taxish20150401_STODp145;
FROM taxish20150401_STTP145
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","145",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg145;
CREATE TABLE taxish20150401_stagg145(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg145
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St145 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg145
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","145",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1145;
CREATE TABLE taxish20150401_agg1145(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St145) bins
INSERT OVERWRITE TABLE taxish20150401_agg1145
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2145;
CREATE TABLE taxish20150401_agg2145(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St145) bins
INSERT OVERWRITE TABLE taxish20150401_agg2145
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1145
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","145",MIN(c),MAX(c);
FROM taxish20150401_agg2145
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","145",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time150;
CREATE TABLE taxish20150401_time150(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 15:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time150
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 15:30:00';

DROP TABLE IF EXISTS taxish20150401_St150;
CREATE EXTERNAL TABLE taxish20150401_St150(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St150
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time150 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time150;
DROP TABLE IF EXISTS taxish20150401_Stmax150;
CREATE TABLE taxish20150401_Stmax150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin150;
CREATE TABLE taxish20150401_Stmin150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St150 t GROUP BY t.carId) ts,taxish20150401_St150 t
INSERT OVERWRITE TABLE taxish20150401_Stmax150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St150 t GROUP BY t.carId) ts,taxish20150401_St150 t
INSERT OVERWRITE TABLE taxish20150401_Stmin150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp150;
CREATE TABLE taxish20150401_STODp150(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin150 tmin,taxish20150401_Stmax150 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp150
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf150;
CREATE TABLE taxish20150401_STODf150(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf150
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp150 od1 JOIN taxish20150401_STODp150 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD150;
CREATE TABLE taxish20150401_STOD150(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD150
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf150
WHERE count>0;
drop table taxish20150401_Stmax150;
drop table taxish20150401_Stmin150;
drop table taxish20150401_STODf150;
FROM taxish20150401_STOD150
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","150",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO150;
CREATE TABLE taxish20150401_STO150(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD150;
CREATE TABLE taxish20150401_STD150(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO150
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp150
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD150
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp150
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP150;
CREATE TABLE taxish20150401_STTP150(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO150 o,taxish20150401_STD150 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP150
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO150;
drop table taxish20150401_STD150;
drop table taxish20150401_STODp150;
FROM taxish20150401_STTP150
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","150",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg150;
CREATE TABLE taxish20150401_stagg150(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg150
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St150 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg150
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","150",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1150;
CREATE TABLE taxish20150401_agg1150(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St150) bins
INSERT OVERWRITE TABLE taxish20150401_agg1150
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2150;
CREATE TABLE taxish20150401_agg2150(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St150) bins
INSERT OVERWRITE TABLE taxish20150401_agg2150
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1150
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","150",MIN(c),MAX(c);
FROM taxish20150401_agg2150
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","150",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time155;
CREATE TABLE taxish20150401_time155(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 15:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time155
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 16:00:00';

DROP TABLE IF EXISTS taxish20150401_St155;
CREATE EXTERNAL TABLE taxish20150401_St155(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St155
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time155 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time155;
DROP TABLE IF EXISTS taxish20150401_Stmax155;
CREATE TABLE taxish20150401_Stmax155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin155;
CREATE TABLE taxish20150401_Stmin155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St155 t GROUP BY t.carId) ts,taxish20150401_St155 t
INSERT OVERWRITE TABLE taxish20150401_Stmax155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St155 t GROUP BY t.carId) ts,taxish20150401_St155 t
INSERT OVERWRITE TABLE taxish20150401_Stmin155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp155;
CREATE TABLE taxish20150401_STODp155(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin155 tmin,taxish20150401_Stmax155 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp155
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf155;
CREATE TABLE taxish20150401_STODf155(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf155
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp155 od1 JOIN taxish20150401_STODp155 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD155;
CREATE TABLE taxish20150401_STOD155(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD155
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf155
WHERE count>0;
drop table taxish20150401_Stmax155;
drop table taxish20150401_Stmin155;
drop table taxish20150401_STODf155;
FROM taxish20150401_STOD155
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","155",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO155;
CREATE TABLE taxish20150401_STO155(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD155;
CREATE TABLE taxish20150401_STD155(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO155
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp155
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD155
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp155
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP155;
CREATE TABLE taxish20150401_STTP155(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO155 o,taxish20150401_STD155 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP155
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO155;
drop table taxish20150401_STD155;
drop table taxish20150401_STODp155;
FROM taxish20150401_STTP155
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","155",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg155;
CREATE TABLE taxish20150401_stagg155(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg155
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St155 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg155
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","155",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1155;
CREATE TABLE taxish20150401_agg1155(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St155) bins
INSERT OVERWRITE TABLE taxish20150401_agg1155
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2155;
CREATE TABLE taxish20150401_agg2155(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St155) bins
INSERT OVERWRITE TABLE taxish20150401_agg2155
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1155
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","155",MIN(c),MAX(c);
FROM taxish20150401_agg2155
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","155",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time160;
CREATE TABLE taxish20150401_time160(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 16:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time160
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 16:30:00';

DROP TABLE IF EXISTS taxish20150401_St160;
CREATE EXTERNAL TABLE taxish20150401_St160(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St160
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time160 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time160;
DROP TABLE IF EXISTS taxish20150401_Stmax160;
CREATE TABLE taxish20150401_Stmax160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin160;
CREATE TABLE taxish20150401_Stmin160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St160 t GROUP BY t.carId) ts,taxish20150401_St160 t
INSERT OVERWRITE TABLE taxish20150401_Stmax160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St160 t GROUP BY t.carId) ts,taxish20150401_St160 t
INSERT OVERWRITE TABLE taxish20150401_Stmin160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp160;
CREATE TABLE taxish20150401_STODp160(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin160 tmin,taxish20150401_Stmax160 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp160
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf160;
CREATE TABLE taxish20150401_STODf160(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf160
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp160 od1 JOIN taxish20150401_STODp160 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD160;
CREATE TABLE taxish20150401_STOD160(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD160
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf160
WHERE count>0;
drop table taxish20150401_Stmax160;
drop table taxish20150401_Stmin160;
drop table taxish20150401_STODf160;
FROM taxish20150401_STOD160
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","160",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO160;
CREATE TABLE taxish20150401_STO160(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD160;
CREATE TABLE taxish20150401_STD160(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO160
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp160
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD160
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp160
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP160;
CREATE TABLE taxish20150401_STTP160(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO160 o,taxish20150401_STD160 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP160
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO160;
drop table taxish20150401_STD160;
drop table taxish20150401_STODp160;
FROM taxish20150401_STTP160
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","160",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg160;
CREATE TABLE taxish20150401_stagg160(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg160
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St160 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg160
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","160",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1160;
CREATE TABLE taxish20150401_agg1160(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St160) bins
INSERT OVERWRITE TABLE taxish20150401_agg1160
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2160;
CREATE TABLE taxish20150401_agg2160(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St160) bins
INSERT OVERWRITE TABLE taxish20150401_agg2160
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1160
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","160",MIN(c),MAX(c);
FROM taxish20150401_agg2160
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","160",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time165;
CREATE TABLE taxish20150401_time165(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 16:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time165
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 17:00:00';

DROP TABLE IF EXISTS taxish20150401_St165;
CREATE EXTERNAL TABLE taxish20150401_St165(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St165
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time165 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time165;
DROP TABLE IF EXISTS taxish20150401_Stmax165;
CREATE TABLE taxish20150401_Stmax165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin165;
CREATE TABLE taxish20150401_Stmin165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St165 t GROUP BY t.carId) ts,taxish20150401_St165 t
INSERT OVERWRITE TABLE taxish20150401_Stmax165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St165 t GROUP BY t.carId) ts,taxish20150401_St165 t
INSERT OVERWRITE TABLE taxish20150401_Stmin165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp165;
CREATE TABLE taxish20150401_STODp165(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin165 tmin,taxish20150401_Stmax165 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp165
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf165;
CREATE TABLE taxish20150401_STODf165(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf165
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp165 od1 JOIN taxish20150401_STODp165 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD165;
CREATE TABLE taxish20150401_STOD165(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD165
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf165
WHERE count>0;
drop table taxish20150401_Stmax165;
drop table taxish20150401_Stmin165;
drop table taxish20150401_STODf165;
FROM taxish20150401_STOD165
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","165",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO165;
CREATE TABLE taxish20150401_STO165(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD165;
CREATE TABLE taxish20150401_STD165(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO165
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp165
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD165
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp165
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP165;
CREATE TABLE taxish20150401_STTP165(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO165 o,taxish20150401_STD165 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP165
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO165;
drop table taxish20150401_STD165;
drop table taxish20150401_STODp165;
FROM taxish20150401_STTP165
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","165",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg165;
CREATE TABLE taxish20150401_stagg165(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg165
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St165 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg165
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","165",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1165;
CREATE TABLE taxish20150401_agg1165(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St165) bins
INSERT OVERWRITE TABLE taxish20150401_agg1165
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2165;
CREATE TABLE taxish20150401_agg2165(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St165) bins
INSERT OVERWRITE TABLE taxish20150401_agg2165
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1165
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","165",MIN(c),MAX(c);
FROM taxish20150401_agg2165
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","165",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time170;
CREATE TABLE taxish20150401_time170(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 17:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time170
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 17:30:00';

DROP TABLE IF EXISTS taxish20150401_St170;
CREATE EXTERNAL TABLE taxish20150401_St170(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St170
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time170 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time170;
DROP TABLE IF EXISTS taxish20150401_Stmax170;
CREATE TABLE taxish20150401_Stmax170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin170;
CREATE TABLE taxish20150401_Stmin170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St170 t GROUP BY t.carId) ts,taxish20150401_St170 t
INSERT OVERWRITE TABLE taxish20150401_Stmax170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St170 t GROUP BY t.carId) ts,taxish20150401_St170 t
INSERT OVERWRITE TABLE taxish20150401_Stmin170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp170;
CREATE TABLE taxish20150401_STODp170(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin170 tmin,taxish20150401_Stmax170 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp170
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf170;
CREATE TABLE taxish20150401_STODf170(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf170
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp170 od1 JOIN taxish20150401_STODp170 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD170;
CREATE TABLE taxish20150401_STOD170(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD170
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf170
WHERE count>0;
drop table taxish20150401_Stmax170;
drop table taxish20150401_Stmin170;
drop table taxish20150401_STODf170;
FROM taxish20150401_STOD170
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","170",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO170;
CREATE TABLE taxish20150401_STO170(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD170;
CREATE TABLE taxish20150401_STD170(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO170
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp170
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD170
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp170
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP170;
CREATE TABLE taxish20150401_STTP170(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO170 o,taxish20150401_STD170 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP170
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO170;
drop table taxish20150401_STD170;
drop table taxish20150401_STODp170;
FROM taxish20150401_STTP170
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","170",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg170;
CREATE TABLE taxish20150401_stagg170(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg170
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St170 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg170
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","170",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1170;
CREATE TABLE taxish20150401_agg1170(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St170) bins
INSERT OVERWRITE TABLE taxish20150401_agg1170
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2170;
CREATE TABLE taxish20150401_agg2170(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St170) bins
INSERT OVERWRITE TABLE taxish20150401_agg2170
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1170
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","170",MIN(c),MAX(c);
FROM taxish20150401_agg2170
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","170",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time175;
CREATE TABLE taxish20150401_time175(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 17:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time175
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 18:00:00';

DROP TABLE IF EXISTS taxish20150401_St175;
CREATE EXTERNAL TABLE taxish20150401_St175(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St175
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time175 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time175;
DROP TABLE IF EXISTS taxish20150401_Stmax175;
CREATE TABLE taxish20150401_Stmax175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin175;
CREATE TABLE taxish20150401_Stmin175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St175 t GROUP BY t.carId) ts,taxish20150401_St175 t
INSERT OVERWRITE TABLE taxish20150401_Stmax175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St175 t GROUP BY t.carId) ts,taxish20150401_St175 t
INSERT OVERWRITE TABLE taxish20150401_Stmin175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp175;
CREATE TABLE taxish20150401_STODp175(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin175 tmin,taxish20150401_Stmax175 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp175
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf175;
CREATE TABLE taxish20150401_STODf175(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf175
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp175 od1 JOIN taxish20150401_STODp175 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD175;
CREATE TABLE taxish20150401_STOD175(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD175
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf175
WHERE count>0;
drop table taxish20150401_Stmax175;
drop table taxish20150401_Stmin175;
drop table taxish20150401_STODf175;
FROM taxish20150401_STOD175
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","175",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO175;
CREATE TABLE taxish20150401_STO175(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD175;
CREATE TABLE taxish20150401_STD175(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO175
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp175
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD175
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp175
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP175;
CREATE TABLE taxish20150401_STTP175(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO175 o,taxish20150401_STD175 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP175
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO175;
drop table taxish20150401_STD175;
drop table taxish20150401_STODp175;
FROM taxish20150401_STTP175
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","175",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg175;
CREATE TABLE taxish20150401_stagg175(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg175
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St175 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg175
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","175",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1175;
CREATE TABLE taxish20150401_agg1175(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St175) bins
INSERT OVERWRITE TABLE taxish20150401_agg1175
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2175;
CREATE TABLE taxish20150401_agg2175(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St175) bins
INSERT OVERWRITE TABLE taxish20150401_agg2175
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1175
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","175",MIN(c),MAX(c);
FROM taxish20150401_agg2175
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","175",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time180;
CREATE TABLE taxish20150401_time180(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 18:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time180
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 18:30:00';

DROP TABLE IF EXISTS taxish20150401_St180;
CREATE EXTERNAL TABLE taxish20150401_St180(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St180
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time180 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time180;
DROP TABLE IF EXISTS taxish20150401_Stmax180;
CREATE TABLE taxish20150401_Stmax180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin180;
CREATE TABLE taxish20150401_Stmin180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St180 t GROUP BY t.carId) ts,taxish20150401_St180 t
INSERT OVERWRITE TABLE taxish20150401_Stmax180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St180 t GROUP BY t.carId) ts,taxish20150401_St180 t
INSERT OVERWRITE TABLE taxish20150401_Stmin180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp180;
CREATE TABLE taxish20150401_STODp180(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin180 tmin,taxish20150401_Stmax180 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp180
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf180;
CREATE TABLE taxish20150401_STODf180(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf180
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp180 od1 JOIN taxish20150401_STODp180 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD180;
CREATE TABLE taxish20150401_STOD180(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD180
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf180
WHERE count>0;
drop table taxish20150401_Stmax180;
drop table taxish20150401_Stmin180;
drop table taxish20150401_STODf180;
FROM taxish20150401_STOD180
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","180",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO180;
CREATE TABLE taxish20150401_STO180(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD180;
CREATE TABLE taxish20150401_STD180(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO180
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp180
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD180
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp180
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP180;
CREATE TABLE taxish20150401_STTP180(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO180 o,taxish20150401_STD180 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP180
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO180;
drop table taxish20150401_STD180;
drop table taxish20150401_STODp180;
FROM taxish20150401_STTP180
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","180",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg180;
CREATE TABLE taxish20150401_stagg180(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg180
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St180 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg180
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","180",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1180;
CREATE TABLE taxish20150401_agg1180(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St180) bins
INSERT OVERWRITE TABLE taxish20150401_agg1180
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2180;
CREATE TABLE taxish20150401_agg2180(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St180) bins
INSERT OVERWRITE TABLE taxish20150401_agg2180
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1180
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","180",MIN(c),MAX(c);
FROM taxish20150401_agg2180
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","180",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time185;
CREATE TABLE taxish20150401_time185(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 18:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time185
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 19:00:00';

DROP TABLE IF EXISTS taxish20150401_St185;
CREATE EXTERNAL TABLE taxish20150401_St185(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St185
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time185 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time185;
DROP TABLE IF EXISTS taxish20150401_Stmax185;
CREATE TABLE taxish20150401_Stmax185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin185;
CREATE TABLE taxish20150401_Stmin185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmax185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmin185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp185;
CREATE TABLE taxish20150401_STODp185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin185 tmin,taxish20150401_Stmax185 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp185
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf185;
CREATE TABLE taxish20150401_STODf185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf185
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp185 od1 JOIN taxish20150401_STODp185 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD185;
CREATE TABLE taxish20150401_STOD185(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD185
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf185
WHERE count>0;
drop table taxish20150401_Stmax185;
drop table taxish20150401_Stmin185;
drop table taxish20150401_STODf185;
FROM taxish20150401_STOD185
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO185;
CREATE TABLE taxish20150401_STO185(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD185;
CREATE TABLE taxish20150401_STD185(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO185
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp185
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD185
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp185
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP185;
CREATE TABLE taxish20150401_STTP185(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO185 o,taxish20150401_STD185 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP185
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO185;
drop table taxish20150401_STD185;
drop table taxish20150401_STODp185;
FROM taxish20150401_STTP185
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","185",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg185;
CREATE TABLE taxish20150401_stagg185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg185
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St185 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg185
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","185",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1185;
CREATE TABLE taxish20150401_agg1185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St185) bins
INSERT OVERWRITE TABLE taxish20150401_agg1185
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2185;
CREATE TABLE taxish20150401_agg2185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St185) bins
INSERT OVERWRITE TABLE taxish20150401_agg2185
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1185
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(c),MAX(c);
FROM taxish20150401_agg2185
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time190;
CREATE TABLE taxish20150401_time190(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 19:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time190
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 19:30:00';

DROP TABLE IF EXISTS taxish20150401_St190;
CREATE EXTERNAL TABLE taxish20150401_St190(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St190
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time190 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time190;
DROP TABLE IF EXISTS taxish20150401_Stmax190;
CREATE TABLE taxish20150401_Stmax190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin190;
CREATE TABLE taxish20150401_Stmin190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St190 t GROUP BY t.carId) ts,taxish20150401_St190 t
INSERT OVERWRITE TABLE taxish20150401_Stmax190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St190 t GROUP BY t.carId) ts,taxish20150401_St190 t
INSERT OVERWRITE TABLE taxish20150401_Stmin190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp190;
CREATE TABLE taxish20150401_STODp190(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin190 tmin,taxish20150401_Stmax190 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp190
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf190;
CREATE TABLE taxish20150401_STODf190(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf190
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp190 od1 JOIN taxish20150401_STODp190 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD190;
CREATE TABLE taxish20150401_STOD190(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD190
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf190
WHERE count>0;
drop table taxish20150401_Stmax190;
drop table taxish20150401_Stmin190;
drop table taxish20150401_STODf190;
FROM taxish20150401_STOD190
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","190",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO190;
CREATE TABLE taxish20150401_STO190(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD190;
CREATE TABLE taxish20150401_STD190(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO190
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp190
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD190
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp190
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP190;
CREATE TABLE taxish20150401_STTP190(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO190 o,taxish20150401_STD190 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP190
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO190;
drop table taxish20150401_STD190;
drop table taxish20150401_STODp190;
FROM taxish20150401_STTP190
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","190",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg190;
CREATE TABLE taxish20150401_stagg190(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg190
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St190 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg190
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","190",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1190;
CREATE TABLE taxish20150401_agg1190(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St190) bins
INSERT OVERWRITE TABLE taxish20150401_agg1190
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2190;
CREATE TABLE taxish20150401_agg2190(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St190) bins
INSERT OVERWRITE TABLE taxish20150401_agg2190
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1190
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","190",MIN(c),MAX(c);
FROM taxish20150401_agg2190
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","190",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time195;
CREATE TABLE taxish20150401_time195(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 19:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time195
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 20:00:00';

DROP TABLE IF EXISTS taxish20150401_St195;
CREATE EXTERNAL TABLE taxish20150401_St195(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St195
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time195 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time195;
DROP TABLE IF EXISTS taxish20150401_Stmax195;
CREATE TABLE taxish20150401_Stmax195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin195;
CREATE TABLE taxish20150401_Stmin195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St195 t GROUP BY t.carId) ts,taxish20150401_St195 t
INSERT OVERWRITE TABLE taxish20150401_Stmax195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St195 t GROUP BY t.carId) ts,taxish20150401_St195 t
INSERT OVERWRITE TABLE taxish20150401_Stmin195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp195;
CREATE TABLE taxish20150401_STODp195(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin195 tmin,taxish20150401_Stmax195 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp195
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf195;
CREATE TABLE taxish20150401_STODf195(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf195
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp195 od1 JOIN taxish20150401_STODp195 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD195;
CREATE TABLE taxish20150401_STOD195(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD195
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf195
WHERE count>0;
drop table taxish20150401_Stmax195;
drop table taxish20150401_Stmin195;
drop table taxish20150401_STODf195;
FROM taxish20150401_STOD195
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","195",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO195;
CREATE TABLE taxish20150401_STO195(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD195;
CREATE TABLE taxish20150401_STD195(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO195
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp195
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD195
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp195
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP195;
CREATE TABLE taxish20150401_STTP195(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO195 o,taxish20150401_STD195 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP195
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO195;
drop table taxish20150401_STD195;
drop table taxish20150401_STODp195;
FROM taxish20150401_STTP195
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","195",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg195;
CREATE TABLE taxish20150401_stagg195(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg195
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St195 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg195
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","195",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1195;
CREATE TABLE taxish20150401_agg1195(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St195) bins
INSERT OVERWRITE TABLE taxish20150401_agg1195
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2195;
CREATE TABLE taxish20150401_agg2195(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St195) bins
INSERT OVERWRITE TABLE taxish20150401_agg2195
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1195
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","195",MIN(c),MAX(c);
FROM taxish20150401_agg2195
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","195",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time200;
CREATE TABLE taxish20150401_time200(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 20:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time200
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 20:30:00';

DROP TABLE IF EXISTS taxish20150401_St200;
CREATE EXTERNAL TABLE taxish20150401_St200(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St200
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time200 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time200;
DROP TABLE IF EXISTS taxish20150401_Stmax200;
CREATE TABLE taxish20150401_Stmax200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin200;
CREATE TABLE taxish20150401_Stmin200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St200 t GROUP BY t.carId) ts,taxish20150401_St200 t
INSERT OVERWRITE TABLE taxish20150401_Stmax200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St200 t GROUP BY t.carId) ts,taxish20150401_St200 t
INSERT OVERWRITE TABLE taxish20150401_Stmin200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp200;
CREATE TABLE taxish20150401_STODp200(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin200 tmin,taxish20150401_Stmax200 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp200
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf200;
CREATE TABLE taxish20150401_STODf200(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf200
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp200 od1 JOIN taxish20150401_STODp200 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD200;
CREATE TABLE taxish20150401_STOD200(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD200
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf200
WHERE count>0;
drop table taxish20150401_Stmax200;
drop table taxish20150401_Stmin200;
drop table taxish20150401_STODf200;
FROM taxish20150401_STOD200
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","200",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO200;
CREATE TABLE taxish20150401_STO200(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD200;
CREATE TABLE taxish20150401_STD200(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO200
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp200
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD200
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp200
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP200;
CREATE TABLE taxish20150401_STTP200(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO200 o,taxish20150401_STD200 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP200
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO200;
drop table taxish20150401_STD200;
drop table taxish20150401_STODp200;
FROM taxish20150401_STTP200
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","200",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg200;
CREATE TABLE taxish20150401_stagg200(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg200
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St200 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg200
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","200",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1200;
CREATE TABLE taxish20150401_agg1200(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St200) bins
INSERT OVERWRITE TABLE taxish20150401_agg1200
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2200;
CREATE TABLE taxish20150401_agg2200(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St200) bins
INSERT OVERWRITE TABLE taxish20150401_agg2200
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1200
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","200",MIN(c),MAX(c);
FROM taxish20150401_agg2200
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","200",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time205;
CREATE TABLE taxish20150401_time205(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 20:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time205
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 21:00:00';

DROP TABLE IF EXISTS taxish20150401_St205;
CREATE EXTERNAL TABLE taxish20150401_St205(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St205
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time205 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time205;
DROP TABLE IF EXISTS taxish20150401_Stmax205;
CREATE TABLE taxish20150401_Stmax205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin205;
CREATE TABLE taxish20150401_Stmin205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St205 t GROUP BY t.carId) ts,taxish20150401_St205 t
INSERT OVERWRITE TABLE taxish20150401_Stmax205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St205 t GROUP BY t.carId) ts,taxish20150401_St205 t
INSERT OVERWRITE TABLE taxish20150401_Stmin205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp205;
CREATE TABLE taxish20150401_STODp205(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin205 tmin,taxish20150401_Stmax205 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp205
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf205;
CREATE TABLE taxish20150401_STODf205(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf205
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp205 od1 JOIN taxish20150401_STODp205 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD205;
CREATE TABLE taxish20150401_STOD205(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD205
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf205
WHERE count>0;
drop table taxish20150401_Stmax205;
drop table taxish20150401_Stmin205;
drop table taxish20150401_STODf205;
FROM taxish20150401_STOD205
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","205",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO205;
CREATE TABLE taxish20150401_STO205(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD205;
CREATE TABLE taxish20150401_STD205(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO205
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp205
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD205
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp205
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP205;
CREATE TABLE taxish20150401_STTP205(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO205 o,taxish20150401_STD205 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP205
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO205;
drop table taxish20150401_STD205;
drop table taxish20150401_STODp205;
FROM taxish20150401_STTP205
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","205",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg205;
CREATE TABLE taxish20150401_stagg205(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg205
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St205 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg205
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","205",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1205;
CREATE TABLE taxish20150401_agg1205(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St205) bins
INSERT OVERWRITE TABLE taxish20150401_agg1205
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2205;
CREATE TABLE taxish20150401_agg2205(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St205) bins
INSERT OVERWRITE TABLE taxish20150401_agg2205
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1205
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","205",MIN(c),MAX(c);
FROM taxish20150401_agg2205
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","205",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time210;
CREATE TABLE taxish20150401_time210(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 21:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time210
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 21:30:00';

DROP TABLE IF EXISTS taxish20150401_St210;
CREATE EXTERNAL TABLE taxish20150401_St210(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St210
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time210 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time210;
DROP TABLE IF EXISTS taxish20150401_Stmax210;
CREATE TABLE taxish20150401_Stmax210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin210;
CREATE TABLE taxish20150401_Stmin210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St210 t GROUP BY t.carId) ts,taxish20150401_St210 t
INSERT OVERWRITE TABLE taxish20150401_Stmax210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St210 t GROUP BY t.carId) ts,taxish20150401_St210 t
INSERT OVERWRITE TABLE taxish20150401_Stmin210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp210;
CREATE TABLE taxish20150401_STODp210(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin210 tmin,taxish20150401_Stmax210 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp210
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf210;
CREATE TABLE taxish20150401_STODf210(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf210
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp210 od1 JOIN taxish20150401_STODp210 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD210;
CREATE TABLE taxish20150401_STOD210(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD210
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf210
WHERE count>0;
drop table taxish20150401_Stmax210;
drop table taxish20150401_Stmin210;
drop table taxish20150401_STODf210;
FROM taxish20150401_STOD210
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","210",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO210;
CREATE TABLE taxish20150401_STO210(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD210;
CREATE TABLE taxish20150401_STD210(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO210
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp210
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD210
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp210
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP210;
CREATE TABLE taxish20150401_STTP210(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO210 o,taxish20150401_STD210 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP210
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO210;
drop table taxish20150401_STD210;
drop table taxish20150401_STODp210;
FROM taxish20150401_STTP210
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","210",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg210;
CREATE TABLE taxish20150401_stagg210(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg210
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St210 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg210
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","210",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1210;
CREATE TABLE taxish20150401_agg1210(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St210) bins
INSERT OVERWRITE TABLE taxish20150401_agg1210
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2210;
CREATE TABLE taxish20150401_agg2210(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St210) bins
INSERT OVERWRITE TABLE taxish20150401_agg2210
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1210
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","210",MIN(c),MAX(c);
FROM taxish20150401_agg2210
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","210",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time215;
CREATE TABLE taxish20150401_time215(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 21:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time215
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 22:00:00';

DROP TABLE IF EXISTS taxish20150401_St215;
CREATE EXTERNAL TABLE taxish20150401_St215(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St215
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time215 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time215;
DROP TABLE IF EXISTS taxish20150401_Stmax215;
CREATE TABLE taxish20150401_Stmax215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin215;
CREATE TABLE taxish20150401_Stmin215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St215 t GROUP BY t.carId) ts,taxish20150401_St215 t
INSERT OVERWRITE TABLE taxish20150401_Stmax215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St215 t GROUP BY t.carId) ts,taxish20150401_St215 t
INSERT OVERWRITE TABLE taxish20150401_Stmin215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp215;
CREATE TABLE taxish20150401_STODp215(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin215 tmin,taxish20150401_Stmax215 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp215
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf215;
CREATE TABLE taxish20150401_STODf215(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf215
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp215 od1 JOIN taxish20150401_STODp215 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD215;
CREATE TABLE taxish20150401_STOD215(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD215
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf215
WHERE count>0;
drop table taxish20150401_Stmax215;
drop table taxish20150401_Stmin215;
drop table taxish20150401_STODf215;
FROM taxish20150401_STOD215
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","215",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO215;
CREATE TABLE taxish20150401_STO215(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD215;
CREATE TABLE taxish20150401_STD215(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO215
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp215
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD215
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp215
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP215;
CREATE TABLE taxish20150401_STTP215(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO215 o,taxish20150401_STD215 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP215
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO215;
drop table taxish20150401_STD215;
drop table taxish20150401_STODp215;
FROM taxish20150401_STTP215
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","215",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg215;
CREATE TABLE taxish20150401_stagg215(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg215
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St215 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg215
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","215",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1215;
CREATE TABLE taxish20150401_agg1215(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St215) bins
INSERT OVERWRITE TABLE taxish20150401_agg1215
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2215;
CREATE TABLE taxish20150401_agg2215(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St215) bins
INSERT OVERWRITE TABLE taxish20150401_agg2215
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1215
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","215",MIN(c),MAX(c);
FROM taxish20150401_agg2215
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","215",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time220;
CREATE TABLE taxish20150401_time220(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 22:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time220
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 22:30:00';

DROP TABLE IF EXISTS taxish20150401_St220;
CREATE EXTERNAL TABLE taxish20150401_St220(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St220
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time220 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time220;
DROP TABLE IF EXISTS taxish20150401_Stmax220;
CREATE TABLE taxish20150401_Stmax220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin220;
CREATE TABLE taxish20150401_Stmin220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St220 t GROUP BY t.carId) ts,taxish20150401_St220 t
INSERT OVERWRITE TABLE taxish20150401_Stmax220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St220 t GROUP BY t.carId) ts,taxish20150401_St220 t
INSERT OVERWRITE TABLE taxish20150401_Stmin220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp220;
CREATE TABLE taxish20150401_STODp220(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin220 tmin,taxish20150401_Stmax220 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp220
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf220;
CREATE TABLE taxish20150401_STODf220(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf220
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp220 od1 JOIN taxish20150401_STODp220 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD220;
CREATE TABLE taxish20150401_STOD220(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD220
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf220
WHERE count>0;
drop table taxish20150401_Stmax220;
drop table taxish20150401_Stmin220;
drop table taxish20150401_STODf220;
FROM taxish20150401_STOD220
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","220",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO220;
CREATE TABLE taxish20150401_STO220(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD220;
CREATE TABLE taxish20150401_STD220(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO220
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp220
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD220
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp220
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP220;
CREATE TABLE taxish20150401_STTP220(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO220 o,taxish20150401_STD220 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP220
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO220;
drop table taxish20150401_STD220;
drop table taxish20150401_STODp220;
FROM taxish20150401_STTP220
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","220",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg220;
CREATE TABLE taxish20150401_stagg220(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg220
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St220 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg220
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","220",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1220;
CREATE TABLE taxish20150401_agg1220(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St220) bins
INSERT OVERWRITE TABLE taxish20150401_agg1220
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2220;
CREATE TABLE taxish20150401_agg2220(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St220) bins
INSERT OVERWRITE TABLE taxish20150401_agg2220
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1220
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","220",MIN(c),MAX(c);
FROM taxish20150401_agg2220
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","220",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time225;
CREATE TABLE taxish20150401_time225(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 22:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time225
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 23:00:00';

DROP TABLE IF EXISTS taxish20150401_St225;
CREATE EXTERNAL TABLE taxish20150401_St225(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St225
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time225 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time225;
DROP TABLE IF EXISTS taxish20150401_Stmax225;
CREATE TABLE taxish20150401_Stmax225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin225;
CREATE TABLE taxish20150401_Stmin225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St225 t GROUP BY t.carId) ts,taxish20150401_St225 t
INSERT OVERWRITE TABLE taxish20150401_Stmax225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St225 t GROUP BY t.carId) ts,taxish20150401_St225 t
INSERT OVERWRITE TABLE taxish20150401_Stmin225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp225;
CREATE TABLE taxish20150401_STODp225(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin225 tmin,taxish20150401_Stmax225 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp225
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf225;
CREATE TABLE taxish20150401_STODf225(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf225
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp225 od1 JOIN taxish20150401_STODp225 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD225;
CREATE TABLE taxish20150401_STOD225(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD225
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf225
WHERE count>0;
drop table taxish20150401_Stmax225;
drop table taxish20150401_Stmin225;
drop table taxish20150401_STODf225;
FROM taxish20150401_STOD225
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","225",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO225;
CREATE TABLE taxish20150401_STO225(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD225;
CREATE TABLE taxish20150401_STD225(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO225
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp225
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD225
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp225
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP225;
CREATE TABLE taxish20150401_STTP225(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO225 o,taxish20150401_STD225 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP225
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO225;
drop table taxish20150401_STD225;
drop table taxish20150401_STODp225;
FROM taxish20150401_STTP225
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","225",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg225;
CREATE TABLE taxish20150401_stagg225(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg225
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St225 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg225
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","225",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1225;
CREATE TABLE taxish20150401_agg1225(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St225) bins
INSERT OVERWRITE TABLE taxish20150401_agg1225
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2225;
CREATE TABLE taxish20150401_agg2225(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St225) bins
INSERT OVERWRITE TABLE taxish20150401_agg2225
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1225
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","225",MIN(c),MAX(c);
FROM taxish20150401_agg2225
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","225",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time230;
CREATE TABLE taxish20150401_time230(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 23:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time230
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 23:30:00';

DROP TABLE IF EXISTS taxish20150401_St230;
CREATE EXTERNAL TABLE taxish20150401_St230(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St230
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time230 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time230;
DROP TABLE IF EXISTS taxish20150401_Stmax230;
CREATE TABLE taxish20150401_Stmax230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin230;
CREATE TABLE taxish20150401_Stmin230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St230 t GROUP BY t.carId) ts,taxish20150401_St230 t
INSERT OVERWRITE TABLE taxish20150401_Stmax230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St230 t GROUP BY t.carId) ts,taxish20150401_St230 t
INSERT OVERWRITE TABLE taxish20150401_Stmin230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp230;
CREATE TABLE taxish20150401_STODp230(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin230 tmin,taxish20150401_Stmax230 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp230
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf230;
CREATE TABLE taxish20150401_STODf230(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf230
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp230 od1 JOIN taxish20150401_STODp230 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD230;
CREATE TABLE taxish20150401_STOD230(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD230
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf230
WHERE count>0;
drop table taxish20150401_Stmax230;
drop table taxish20150401_Stmin230;
drop table taxish20150401_STODf230;
FROM taxish20150401_STOD230
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","230",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO230;
CREATE TABLE taxish20150401_STO230(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD230;
CREATE TABLE taxish20150401_STD230(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO230
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp230
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD230
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp230
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP230;
CREATE TABLE taxish20150401_STTP230(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO230 o,taxish20150401_STD230 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP230
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO230;
drop table taxish20150401_STD230;
drop table taxish20150401_STODp230;
FROM taxish20150401_STTP230
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","230",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg230;
CREATE TABLE taxish20150401_stagg230(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg230
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St230 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg230
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","230",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1230;
CREATE TABLE taxish20150401_agg1230(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St230) bins
INSERT OVERWRITE TABLE taxish20150401_agg1230
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2230;
CREATE TABLE taxish20150401_agg2230(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St230) bins
INSERT OVERWRITE TABLE taxish20150401_agg2230
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1230
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","230",MIN(c),MAX(c);
FROM taxish20150401_agg2230
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","230",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_time235;
CREATE TABLE taxish20150401_time235(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 23:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time235
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 24:00:00';

DROP TABLE IF EXISTS taxish20150401_St235;
CREATE EXTERNAL TABLE taxish20150401_St235(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St235
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time235 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time235;
DROP TABLE IF EXISTS taxish20150401_Stmax235;
CREATE TABLE taxish20150401_Stmax235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin235;
CREATE TABLE taxish20150401_Stmin235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St235 t GROUP BY t.carId) ts,taxish20150401_St235 t
INSERT OVERWRITE TABLE taxish20150401_Stmax235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St235 t GROUP BY t.carId) ts,taxish20150401_St235 t
INSERT OVERWRITE TABLE taxish20150401_Stmin235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
DROP TABLE IF EXISTS taxish20150401_STODp235;
CREATE TABLE taxish20150401_STODp235(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin235 tmin,taxish20150401_Stmax235 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp235
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
DROP TABLE IF EXISTS taxish20150401_STODf235;
CREATE TABLE taxish20150401_STODf235(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf235
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp235 od1 JOIN taxish20150401_STODp235 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
DROP TABLE IF EXISTS taxish20150401_STOD235;
CREATE TABLE taxish20150401_STOD235(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_STOD235
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf235
WHERE count>0;
drop table taxish20150401_Stmax235;
drop table taxish20150401_Stmin235;
drop table taxish20150401_STODf235;
FROM taxish20150401_STOD235
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","235",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_STO235;
CREATE TABLE taxish20150401_STO235(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD235;
CREATE TABLE taxish20150401_STD235(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO235
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp235
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD235
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp235
GROUP BY DctOBJECTID,Dctcx,Dctcy;
DROP TABLE IF EXISTS taxish20150401_STTP235;
CREATE TABLE taxish20150401_STTP235(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM taxish20150401_STO235 o,taxish20150401_STD235 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP235
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
drop table taxish20150401_STO235;
drop table taxish20150401_STD235;
drop table taxish20150401_STODp235;
FROM taxish20150401_STTP235
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","235",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_stagg235;
CREATE TABLE taxish20150401_stagg235(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_stagg235
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St235 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
FROM taxish20150401_stagg235
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","235",MIN(c),MAX(c);

DROP TABLE IF EXISTS taxish20150401_agg1235;
CREATE TABLE taxish20150401_agg1235(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St235) bins
INSERT OVERWRITE TABLE taxish20150401_agg1235
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
DROP TABLE IF EXISTS taxish20150401_agg2235;
CREATE TABLE taxish20150401_agg2235(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St235) bins
INSERT OVERWRITE TABLE taxish20150401_agg2235
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1235
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","235",MIN(c),MAX(c);
FROM taxish20150401_agg2235
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","235",MIN(c),MAX(c);

INSERT OVERWRITE TABLE taxish20150401_value 
SELECT * FROM taxish20150401_valuep;
drop table taxish20150401_valuep;
