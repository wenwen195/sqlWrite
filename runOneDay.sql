add jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar;
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';

CREATE EXTERNAL TABLE blocksh_v1p (Name string, objectid string, cx DOUBLE,cy DOUBLE,BoundaryShape binary)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
LOAD DATA INPATH  '/SHstreet/enCounty.json' OVERWRITE INTO TABLE blocksh_v1p;

CREATE EXTERNAL TABLE taxish20150401_(carId DOUBLE,isAlarm DOUBLE,isEmpty DOUBLE,topLight DOUBLE,
Elevated DOUBLE,isBrake DOUBLE,receiveTime TIMESTAMP,GPSTime STRING,longitude DOUBLE,latitude DOUBLE,
speed DOUBLE,direction DOUBLE,satellite DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile
tblproperties ("skip.header.line.count"="1");
describe taxish20150401_;
LOAD DATA INPATH '/taxidemo/part-4.01.csv' OVERWRITE INTO TABLE taxish20150401_;

CREATE TABLE taxish20150401_value(type STRING,time STRING,min DOUBLE,max DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
CREATE TABLE taxish20150401_valuep(type STRING,time STRING,min DOUBLE,max DOUBLE);

CREATE TABLE taxish20150401_time000(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time000;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 00:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time000
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 00:30:00';

CREATE EXTERNAL TABLE taxish20150401_St000(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St000
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time000 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time000;
CREATE TABLE taxish20150401_Stmax000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin000(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St000 t GROUP BY t.carId) ts,taxish20150401_St000 t
INSERT OVERWRITE TABLE taxish20150401_Stmax000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St000 t GROUP BY t.carId) ts,taxish20150401_St000 t
INSERT OVERWRITE TABLE taxish20150401_Stmin000
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp000(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin000 tmin,taxish20150401_Stmax000 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp000
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf000(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf000
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp000 od1 JOIN taxish20150401_STODp000 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD000(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD000
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf000WHERE count>0;
drop table taxish20150401_Stmax000;
drop table taxish20150401_Stmin000;
drop table taxish20150401_STODf000;
FROM taxish20150401_STOD000INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO000(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD000(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO000
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp000GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD000
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp000GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP000(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP000INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","000",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO000(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD000(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO000
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp000GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD000
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp000GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP000(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg000INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1000(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St000) bins
INSERT OVERWRITE TABLE taxish20150401_agg1000
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2000(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St000) bins
INSERT OVERWRITE TABLE taxish20150401_agg2000
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1000INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2000INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time005(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time005;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 00:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time005
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 01:00:00';

CREATE EXTERNAL TABLE taxish20150401_St005(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St005
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time005 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time005;
CREATE TABLE taxish20150401_Stmax005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin005(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St005 t GROUP BY t.carId) ts,taxish20150401_St005 t
INSERT OVERWRITE TABLE taxish20150401_Stmax005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St005 t GROUP BY t.carId) ts,taxish20150401_St005 t
INSERT OVERWRITE TABLE taxish20150401_Stmin005
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp005(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin005 tmin,taxish20150401_Stmax005 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp005
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf005(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf005
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp005 od1 JOIN taxish20150401_STODp005 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD005(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD005
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf005WHERE count>0;
drop table taxish20150401_Stmax005;
drop table taxish20150401_Stmin005;
drop table taxish20150401_STODf005;
FROM taxish20150401_STOD005INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO005(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD005(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO005
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp005GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD005
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp005GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP005(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP005INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","005",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO005(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD005(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO005
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp005GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD005
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp005GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP005(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg005INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1005(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St005) bins
INSERT OVERWRITE TABLE taxish20150401_agg1005
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2005(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St005) bins
INSERT OVERWRITE TABLE taxish20150401_agg2005
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1005INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2005INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time010(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time010;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 01:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time010
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 01:30:00';

CREATE EXTERNAL TABLE taxish20150401_St010(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St010
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time010 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time010;
CREATE TABLE taxish20150401_Stmax010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin010(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St010 t GROUP BY t.carId) ts,taxish20150401_St010 t
INSERT OVERWRITE TABLE taxish20150401_Stmax010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St010 t GROUP BY t.carId) ts,taxish20150401_St010 t
INSERT OVERWRITE TABLE taxish20150401_Stmin010
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp010(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin010 tmin,taxish20150401_Stmax010 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp010
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf010(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf010
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp010 od1 JOIN taxish20150401_STODp010 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD010(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD010
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf010WHERE count>0;
drop table taxish20150401_Stmax010;
drop table taxish20150401_Stmin010;
drop table taxish20150401_STODf010;
FROM taxish20150401_STOD010INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO010(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD010(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO010
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp010GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD010
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp010GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP010(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP010INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","010",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO010(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD010(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO010
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp010GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD010
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp010GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP010(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg010INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1010(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St010) bins
INSERT OVERWRITE TABLE taxish20150401_agg1010
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2010(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St010) bins
INSERT OVERWRITE TABLE taxish20150401_agg2010
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1010INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2010INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time015(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time015;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 01:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time015
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 02:00:00';

CREATE EXTERNAL TABLE taxish20150401_St015(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St015
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time015 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time015;
CREATE TABLE taxish20150401_Stmax015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin015(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St015 t GROUP BY t.carId) ts,taxish20150401_St015 t
INSERT OVERWRITE TABLE taxish20150401_Stmax015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St015 t GROUP BY t.carId) ts,taxish20150401_St015 t
INSERT OVERWRITE TABLE taxish20150401_Stmin015
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp015(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin015 tmin,taxish20150401_Stmax015 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp015
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf015(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf015
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp015 od1 JOIN taxish20150401_STODp015 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD015(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD015
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf015WHERE count>0;
drop table taxish20150401_Stmax015;
drop table taxish20150401_Stmin015;
drop table taxish20150401_STODf015;
FROM taxish20150401_STOD015INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO015(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD015(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO015
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp015GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD015
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp015GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP015(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP015INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","015",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO015(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD015(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO015
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp015GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD015
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp015GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP015(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg015INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1015(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St015) bins
INSERT OVERWRITE TABLE taxish20150401_agg1015
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2015(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St015) bins
INSERT OVERWRITE TABLE taxish20150401_agg2015
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1015INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2015INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time020(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time020;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 02:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time020
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 02:30:00';

CREATE EXTERNAL TABLE taxish20150401_St020(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St020
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time020 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time020;
CREATE TABLE taxish20150401_Stmax020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin020(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St020 t GROUP BY t.carId) ts,taxish20150401_St020 t
INSERT OVERWRITE TABLE taxish20150401_Stmax020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St020 t GROUP BY t.carId) ts,taxish20150401_St020 t
INSERT OVERWRITE TABLE taxish20150401_Stmin020
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp020(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin020 tmin,taxish20150401_Stmax020 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp020
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf020(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf020
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp020 od1 JOIN taxish20150401_STODp020 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD020(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD020
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf020WHERE count>0;
drop table taxish20150401_Stmax020;
drop table taxish20150401_Stmin020;
drop table taxish20150401_STODf020;
FROM taxish20150401_STOD020INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO020(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD020(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO020
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp020GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD020
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp020GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP020(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP020INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","020",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO020(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD020(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO020
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp020GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD020
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp020GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP020(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg020INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1020(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St020) bins
INSERT OVERWRITE TABLE taxish20150401_agg1020
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2020(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St020) bins
INSERT OVERWRITE TABLE taxish20150401_agg2020
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1020INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2020INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time025(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time025;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 02:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time025
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 03:00:00';

CREATE EXTERNAL TABLE taxish20150401_St025(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St025
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time025 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time025;
CREATE TABLE taxish20150401_Stmax025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin025(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St025 t GROUP BY t.carId) ts,taxish20150401_St025 t
INSERT OVERWRITE TABLE taxish20150401_Stmax025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St025 t GROUP BY t.carId) ts,taxish20150401_St025 t
INSERT OVERWRITE TABLE taxish20150401_Stmin025
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp025(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin025 tmin,taxish20150401_Stmax025 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp025
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf025(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf025
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp025 od1 JOIN taxish20150401_STODp025 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD025(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD025
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf025WHERE count>0;
drop table taxish20150401_Stmax025;
drop table taxish20150401_Stmin025;
drop table taxish20150401_STODf025;
FROM taxish20150401_STOD025INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO025(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD025(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO025
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp025GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD025
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp025GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP025(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP025INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","025",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO025(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD025(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO025
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp025GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD025
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp025GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP025(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg025INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1025(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St025) bins
INSERT OVERWRITE TABLE taxish20150401_agg1025
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2025(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St025) bins
INSERT OVERWRITE TABLE taxish20150401_agg2025
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1025INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2025INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time030(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time030;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 03:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time030
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 03:30:00';

CREATE EXTERNAL TABLE taxish20150401_St030(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St030
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time030 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time030;
CREATE TABLE taxish20150401_Stmax030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin030(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St030 t GROUP BY t.carId) ts,taxish20150401_St030 t
INSERT OVERWRITE TABLE taxish20150401_Stmax030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St030 t GROUP BY t.carId) ts,taxish20150401_St030 t
INSERT OVERWRITE TABLE taxish20150401_Stmin030
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp030(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin030 tmin,taxish20150401_Stmax030 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp030
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf030(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf030
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp030 od1 JOIN taxish20150401_STODp030 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD030(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD030
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf030WHERE count>0;
drop table taxish20150401_Stmax030;
drop table taxish20150401_Stmin030;
drop table taxish20150401_STODf030;
FROM taxish20150401_STOD030INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO030(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD030(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO030
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp030GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD030
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp030GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP030(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP030INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","030",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO030(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD030(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO030
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp030GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD030
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp030GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP030(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg030INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1030(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St030) bins
INSERT OVERWRITE TABLE taxish20150401_agg1030
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2030(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St030) bins
INSERT OVERWRITE TABLE taxish20150401_agg2030
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1030INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2030INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time035(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time035;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 03:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time035
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 04:00:00';

CREATE EXTERNAL TABLE taxish20150401_St035(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St035
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time035 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time035;
CREATE TABLE taxish20150401_Stmax035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin035(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St035 t GROUP BY t.carId) ts,taxish20150401_St035 t
INSERT OVERWRITE TABLE taxish20150401_Stmax035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St035 t GROUP BY t.carId) ts,taxish20150401_St035 t
INSERT OVERWRITE TABLE taxish20150401_Stmin035
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp035(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin035 tmin,taxish20150401_Stmax035 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp035
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf035(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf035
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp035 od1 JOIN taxish20150401_STODp035 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD035(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD035
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf035WHERE count>0;
drop table taxish20150401_Stmax035;
drop table taxish20150401_Stmin035;
drop table taxish20150401_STODf035;
FROM taxish20150401_STOD035INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO035(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD035(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO035
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp035GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD035
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp035GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP035(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP035INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","035",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO035(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD035(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO035
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp035GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD035
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp035GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP035(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg035INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1035(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St035) bins
INSERT OVERWRITE TABLE taxish20150401_agg1035
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2035(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St035) bins
INSERT OVERWRITE TABLE taxish20150401_agg2035
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1035INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2035INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time040(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time040;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 04:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time040
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 04:30:00';

CREATE EXTERNAL TABLE taxish20150401_St040(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St040
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time040 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time040;
CREATE TABLE taxish20150401_Stmax040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin040(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St040 t GROUP BY t.carId) ts,taxish20150401_St040 t
INSERT OVERWRITE TABLE taxish20150401_Stmax040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St040 t GROUP BY t.carId) ts,taxish20150401_St040 t
INSERT OVERWRITE TABLE taxish20150401_Stmin040
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp040(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin040 tmin,taxish20150401_Stmax040 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp040
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf040(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf040
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp040 od1 JOIN taxish20150401_STODp040 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD040(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD040
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf040WHERE count>0;
drop table taxish20150401_Stmax040;
drop table taxish20150401_Stmin040;
drop table taxish20150401_STODf040;
FROM taxish20150401_STOD040INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO040(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD040(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO040
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp040GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD040
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp040GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP040(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP040INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","040",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO040(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD040(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO040
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp040GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD040
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp040GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP040(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg040INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1040(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St040) bins
INSERT OVERWRITE TABLE taxish20150401_agg1040
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2040(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St040) bins
INSERT OVERWRITE TABLE taxish20150401_agg2040
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1040INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2040INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time045(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time045;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 04:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time045
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 05:00:00';

CREATE EXTERNAL TABLE taxish20150401_St045(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St045
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time045 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time045;
CREATE TABLE taxish20150401_Stmax045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin045(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St045 t GROUP BY t.carId) ts,taxish20150401_St045 t
INSERT OVERWRITE TABLE taxish20150401_Stmax045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St045 t GROUP BY t.carId) ts,taxish20150401_St045 t
INSERT OVERWRITE TABLE taxish20150401_Stmin045
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp045(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin045 tmin,taxish20150401_Stmax045 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp045
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf045(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf045
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp045 od1 JOIN taxish20150401_STODp045 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD045(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD045
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf045WHERE count>0;
drop table taxish20150401_Stmax045;
drop table taxish20150401_Stmin045;
drop table taxish20150401_STODf045;
FROM taxish20150401_STOD045INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO045(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD045(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO045
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp045GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD045
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp045GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP045(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP045INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","045",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO045(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD045(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO045
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp045GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD045
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp045GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP045(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg045INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1045(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St045) bins
INSERT OVERWRITE TABLE taxish20150401_agg1045
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2045(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St045) bins
INSERT OVERWRITE TABLE taxish20150401_agg2045
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1045INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2045INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time050(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time050;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 05:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time050
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 05:30:00';

CREATE EXTERNAL TABLE taxish20150401_St050(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St050
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time050 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time050;
CREATE TABLE taxish20150401_Stmax050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin050(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St050 t GROUP BY t.carId) ts,taxish20150401_St050 t
INSERT OVERWRITE TABLE taxish20150401_Stmax050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St050 t GROUP BY t.carId) ts,taxish20150401_St050 t
INSERT OVERWRITE TABLE taxish20150401_Stmin050
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp050(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin050 tmin,taxish20150401_Stmax050 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp050
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf050(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf050
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp050 od1 JOIN taxish20150401_STODp050 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD050(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD050
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf050WHERE count>0;
drop table taxish20150401_Stmax050;
drop table taxish20150401_Stmin050;
drop table taxish20150401_STODf050;
FROM taxish20150401_STOD050INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO050(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD050(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO050
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp050GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD050
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp050GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP050(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP050INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","050",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO050(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD050(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO050
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp050GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD050
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp050GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP050(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg050INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1050(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St050) bins
INSERT OVERWRITE TABLE taxish20150401_agg1050
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2050(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St050) bins
INSERT OVERWRITE TABLE taxish20150401_agg2050
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1050INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2050INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time055(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time055;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 05:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time055
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 06:00:00';

CREATE EXTERNAL TABLE taxish20150401_St055(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St055
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time055 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time055;
CREATE TABLE taxish20150401_Stmax055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin055(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St055 t GROUP BY t.carId) ts,taxish20150401_St055 t
INSERT OVERWRITE TABLE taxish20150401_Stmax055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St055 t GROUP BY t.carId) ts,taxish20150401_St055 t
INSERT OVERWRITE TABLE taxish20150401_Stmin055
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp055(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin055 tmin,taxish20150401_Stmax055 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp055
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf055(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf055
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp055 od1 JOIN taxish20150401_STODp055 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD055(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD055
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf055WHERE count>0;
drop table taxish20150401_Stmax055;
drop table taxish20150401_Stmin055;
drop table taxish20150401_STODf055;
FROM taxish20150401_STOD055INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO055(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD055(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO055
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp055GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD055
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp055GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP055(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP055INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","055",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO055(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD055(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO055
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp055GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD055
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp055GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP055(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg055INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1055(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St055) bins
INSERT OVERWRITE TABLE taxish20150401_agg1055
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2055(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St055) bins
INSERT OVERWRITE TABLE taxish20150401_agg2055
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1055INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2055INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time060(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time060;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 06:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time060
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 06:30:00';

CREATE EXTERNAL TABLE taxish20150401_St060(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St060
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time060 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time060;
CREATE TABLE taxish20150401_Stmax060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin060(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St060 t GROUP BY t.carId) ts,taxish20150401_St060 t
INSERT OVERWRITE TABLE taxish20150401_Stmax060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St060 t GROUP BY t.carId) ts,taxish20150401_St060 t
INSERT OVERWRITE TABLE taxish20150401_Stmin060
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp060(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin060 tmin,taxish20150401_Stmax060 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp060
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf060(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf060
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp060 od1 JOIN taxish20150401_STODp060 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD060(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD060
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf060WHERE count>0;
drop table taxish20150401_Stmax060;
drop table taxish20150401_Stmin060;
drop table taxish20150401_STODf060;
FROM taxish20150401_STOD060INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO060(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD060(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO060
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp060GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD060
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp060GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP060(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP060INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","060",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO060(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD060(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO060
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp060GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD060
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp060GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP060(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg060INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1060(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St060) bins
INSERT OVERWRITE TABLE taxish20150401_agg1060
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2060(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St060) bins
INSERT OVERWRITE TABLE taxish20150401_agg2060
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1060INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2060INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time065(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time065;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 06:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time065
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 07:00:00';

CREATE EXTERNAL TABLE taxish20150401_St065(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St065
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time065 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time065;
CREATE TABLE taxish20150401_Stmax065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin065(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St065 t GROUP BY t.carId) ts,taxish20150401_St065 t
INSERT OVERWRITE TABLE taxish20150401_Stmax065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St065 t GROUP BY t.carId) ts,taxish20150401_St065 t
INSERT OVERWRITE TABLE taxish20150401_Stmin065
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp065(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin065 tmin,taxish20150401_Stmax065 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp065
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf065(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf065
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp065 od1 JOIN taxish20150401_STODp065 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD065(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD065
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf065WHERE count>0;
drop table taxish20150401_Stmax065;
drop table taxish20150401_Stmin065;
drop table taxish20150401_STODf065;
FROM taxish20150401_STOD065INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO065(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD065(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO065
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp065GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD065
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp065GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP065(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP065INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","065",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO065(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD065(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO065
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp065GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD065
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp065GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP065(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg065INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1065(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St065) bins
INSERT OVERWRITE TABLE taxish20150401_agg1065
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2065(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St065) bins
INSERT OVERWRITE TABLE taxish20150401_agg2065
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1065INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2065INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time070(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time070;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 07:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time070
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 07:30:00';

CREATE EXTERNAL TABLE taxish20150401_St070(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St070
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time070 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time070;
CREATE TABLE taxish20150401_Stmax070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin070(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St070 t GROUP BY t.carId) ts,taxish20150401_St070 t
INSERT OVERWRITE TABLE taxish20150401_Stmax070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St070 t GROUP BY t.carId) ts,taxish20150401_St070 t
INSERT OVERWRITE TABLE taxish20150401_Stmin070
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp070(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin070 tmin,taxish20150401_Stmax070 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp070
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf070(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf070
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp070 od1 JOIN taxish20150401_STODp070 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD070(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD070
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf070WHERE count>0;
drop table taxish20150401_Stmax070;
drop table taxish20150401_Stmin070;
drop table taxish20150401_STODf070;
FROM taxish20150401_STOD070INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO070(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD070(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO070
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp070GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD070
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp070GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP070(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP070INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","070",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO070(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD070(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO070
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp070GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD070
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp070GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP070(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg070INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1070(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St070) bins
INSERT OVERWRITE TABLE taxish20150401_agg1070
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2070(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St070) bins
INSERT OVERWRITE TABLE taxish20150401_agg2070
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1070INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2070INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time075(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time075;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 07:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time075
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 08:00:00';

CREATE EXTERNAL TABLE taxish20150401_St075(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St075
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time075 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time075;
CREATE TABLE taxish20150401_Stmax075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin075(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St075 t GROUP BY t.carId) ts,taxish20150401_St075 t
INSERT OVERWRITE TABLE taxish20150401_Stmax075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St075 t GROUP BY t.carId) ts,taxish20150401_St075 t
INSERT OVERWRITE TABLE taxish20150401_Stmin075
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp075(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin075 tmin,taxish20150401_Stmax075 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp075
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf075(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf075
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp075 od1 JOIN taxish20150401_STODp075 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD075(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD075
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf075WHERE count>0;
drop table taxish20150401_Stmax075;
drop table taxish20150401_Stmin075;
drop table taxish20150401_STODf075;
FROM taxish20150401_STOD075INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO075(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD075(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO075
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp075GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD075
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp075GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP075(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP075INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","075",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO075(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD075(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO075
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp075GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD075
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp075GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP075(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg075INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1075(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St075) bins
INSERT OVERWRITE TABLE taxish20150401_agg1075
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2075(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St075) bins
INSERT OVERWRITE TABLE taxish20150401_agg2075
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1075INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2075INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time080(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time080;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 08:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time080
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 08:30:00';

CREATE EXTERNAL TABLE taxish20150401_St080(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St080
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time080 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time080;
CREATE TABLE taxish20150401_Stmax080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin080(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St080 t GROUP BY t.carId) ts,taxish20150401_St080 t
INSERT OVERWRITE TABLE taxish20150401_Stmax080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St080 t GROUP BY t.carId) ts,taxish20150401_St080 t
INSERT OVERWRITE TABLE taxish20150401_Stmin080
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp080(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin080 tmin,taxish20150401_Stmax080 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp080
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf080(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf080
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp080 od1 JOIN taxish20150401_STODp080 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD080(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD080
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf080WHERE count>0;
drop table taxish20150401_Stmax080;
drop table taxish20150401_Stmin080;
drop table taxish20150401_STODf080;
FROM taxish20150401_STOD080INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO080(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD080(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO080
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp080GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD080
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp080GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP080(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP080INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","080",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO080(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD080(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO080
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp080GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD080
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp080GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP080(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg080INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1080(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St080) bins
INSERT OVERWRITE TABLE taxish20150401_agg1080
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2080(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St080) bins
INSERT OVERWRITE TABLE taxish20150401_agg2080
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1080INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2080INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time085(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time085;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 08:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time085
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 09:00:00';

CREATE EXTERNAL TABLE taxish20150401_St085(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St085
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time085 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time085;
CREATE TABLE taxish20150401_Stmax085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin085(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St085 t GROUP BY t.carId) ts,taxish20150401_St085 t
INSERT OVERWRITE TABLE taxish20150401_Stmax085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St085 t GROUP BY t.carId) ts,taxish20150401_St085 t
INSERT OVERWRITE TABLE taxish20150401_Stmin085
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp085(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin085 tmin,taxish20150401_Stmax085 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp085
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf085(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf085
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp085 od1 JOIN taxish20150401_STODp085 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD085(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD085
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf085WHERE count>0;
drop table taxish20150401_Stmax085;
drop table taxish20150401_Stmin085;
drop table taxish20150401_STODf085;
FROM taxish20150401_STOD085INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO085(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD085(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO085
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp085GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD085
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp085GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP085(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP085INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","085",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO085(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD085(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO085
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp085GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD085
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp085GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP085(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg085INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1085(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St085) bins
INSERT OVERWRITE TABLE taxish20150401_agg1085
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2085(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St085) bins
INSERT OVERWRITE TABLE taxish20150401_agg2085
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1085INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2085INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time090(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time090;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 09:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time090
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 09:30:00';

CREATE EXTERNAL TABLE taxish20150401_St090(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St090
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time090 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time090;
CREATE TABLE taxish20150401_Stmax090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin090(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St090 t GROUP BY t.carId) ts,taxish20150401_St090 t
INSERT OVERWRITE TABLE taxish20150401_Stmax090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St090 t GROUP BY t.carId) ts,taxish20150401_St090 t
INSERT OVERWRITE TABLE taxish20150401_Stmin090
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp090(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin090 tmin,taxish20150401_Stmax090 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp090
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf090(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf090
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp090 od1 JOIN taxish20150401_STODp090 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD090(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD090
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf090WHERE count>0;
drop table taxish20150401_Stmax090;
drop table taxish20150401_Stmin090;
drop table taxish20150401_STODf090;
FROM taxish20150401_STOD090INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO090(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD090(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO090
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp090GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD090
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp090GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP090(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP090INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","090",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO090(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD090(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO090
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp090GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD090
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp090GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP090(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg090INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1090(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St090) bins
INSERT OVERWRITE TABLE taxish20150401_agg1090
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2090(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St090) bins
INSERT OVERWRITE TABLE taxish20150401_agg2090
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1090INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2090INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time095(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time095;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 09:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time095
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 10:00:00';

CREATE EXTERNAL TABLE taxish20150401_St095(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St095
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time095 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time095;
CREATE TABLE taxish20150401_Stmax095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin095(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St095 t GROUP BY t.carId) ts,taxish20150401_St095 t
INSERT OVERWRITE TABLE taxish20150401_Stmax095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St095 t GROUP BY t.carId) ts,taxish20150401_St095 t
INSERT OVERWRITE TABLE taxish20150401_Stmin095
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp095(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin095 tmin,taxish20150401_Stmax095 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp095
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf095(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf095
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp095 od1 JOIN taxish20150401_STODp095 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD095(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD095
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf095WHERE count>0;
drop table taxish20150401_Stmax095;
drop table taxish20150401_Stmin095;
drop table taxish20150401_STODf095;
FROM taxish20150401_STOD095INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO095(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD095(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO095
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp095GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD095
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp095GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP095(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP095INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","095",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO095(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD095(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO095
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp095GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD095
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp095GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP095(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg095INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1095(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St095) bins
INSERT OVERWRITE TABLE taxish20150401_agg1095
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2095(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St095) bins
INSERT OVERWRITE TABLE taxish20150401_agg2095
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1095INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2095INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time100(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time100;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 10:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time100
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 10:30:00';

CREATE EXTERNAL TABLE taxish20150401_St100(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St100
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time100 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time100;
CREATE TABLE taxish20150401_Stmax100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin100(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St100 t GROUP BY t.carId) ts,taxish20150401_St100 t
INSERT OVERWRITE TABLE taxish20150401_Stmax100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St100 t GROUP BY t.carId) ts,taxish20150401_St100 t
INSERT OVERWRITE TABLE taxish20150401_Stmin100
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp100(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin100 tmin,taxish20150401_Stmax100 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp100
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf100(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf100
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp100 od1 JOIN taxish20150401_STODp100 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD100(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD100
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf100WHERE count>0;
drop table taxish20150401_Stmax100;
drop table taxish20150401_Stmin100;
drop table taxish20150401_STODf100;
FROM taxish20150401_STOD100INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO100(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD100(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO100
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp100GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD100
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp100GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP100(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP100INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","100",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO100(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD100(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO100
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp100GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD100
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp100GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP100(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg100INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1100(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St100) bins
INSERT OVERWRITE TABLE taxish20150401_agg1100
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2100(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St100) bins
INSERT OVERWRITE TABLE taxish20150401_agg2100
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1100INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2100INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time105(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time105;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 10:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time105
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 11:00:00';

CREATE EXTERNAL TABLE taxish20150401_St105(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St105
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time105 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time105;
CREATE TABLE taxish20150401_Stmax105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin105(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St105 t GROUP BY t.carId) ts,taxish20150401_St105 t
INSERT OVERWRITE TABLE taxish20150401_Stmax105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St105 t GROUP BY t.carId) ts,taxish20150401_St105 t
INSERT OVERWRITE TABLE taxish20150401_Stmin105
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp105(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin105 tmin,taxish20150401_Stmax105 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp105
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf105(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf105
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp105 od1 JOIN taxish20150401_STODp105 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD105(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD105
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf105WHERE count>0;
drop table taxish20150401_Stmax105;
drop table taxish20150401_Stmin105;
drop table taxish20150401_STODf105;
FROM taxish20150401_STOD105INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO105(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD105(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO105
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp105GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD105
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp105GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP105(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP105INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","105",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO105(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD105(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO105
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp105GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD105
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp105GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP105(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg105INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1105(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St105) bins
INSERT OVERWRITE TABLE taxish20150401_agg1105
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2105(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St105) bins
INSERT OVERWRITE TABLE taxish20150401_agg2105
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1105INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2105INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time110(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time110;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 11:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time110
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 11:30:00';

CREATE EXTERNAL TABLE taxish20150401_St110(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St110
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time110 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time110;
CREATE TABLE taxish20150401_Stmax110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin110(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St110 t GROUP BY t.carId) ts,taxish20150401_St110 t
INSERT OVERWRITE TABLE taxish20150401_Stmax110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St110 t GROUP BY t.carId) ts,taxish20150401_St110 t
INSERT OVERWRITE TABLE taxish20150401_Stmin110
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp110(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin110 tmin,taxish20150401_Stmax110 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp110
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf110(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf110
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp110 od1 JOIN taxish20150401_STODp110 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD110(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD110
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf110WHERE count>0;
drop table taxish20150401_Stmax110;
drop table taxish20150401_Stmin110;
drop table taxish20150401_STODf110;
FROM taxish20150401_STOD110INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO110(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD110(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO110
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp110GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD110
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp110GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP110(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP110INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","110",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO110(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD110(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO110
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp110GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD110
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp110GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP110(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg110INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1110(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St110) bins
INSERT OVERWRITE TABLE taxish20150401_agg1110
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2110(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St110) bins
INSERT OVERWRITE TABLE taxish20150401_agg2110
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1110INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2110INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time115(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time115;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 11:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time115
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 12:00:00';

CREATE EXTERNAL TABLE taxish20150401_St115(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St115
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time115 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time115;
CREATE TABLE taxish20150401_Stmax115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin115(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St115 t GROUP BY t.carId) ts,taxish20150401_St115 t
INSERT OVERWRITE TABLE taxish20150401_Stmax115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St115 t GROUP BY t.carId) ts,taxish20150401_St115 t
INSERT OVERWRITE TABLE taxish20150401_Stmin115
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp115(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin115 tmin,taxish20150401_Stmax115 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp115
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf115(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf115
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp115 od1 JOIN taxish20150401_STODp115 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD115(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD115
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf115WHERE count>0;
drop table taxish20150401_Stmax115;
drop table taxish20150401_Stmin115;
drop table taxish20150401_STODf115;
FROM taxish20150401_STOD115INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO115(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD115(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO115
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp115GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD115
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp115GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP115(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP115INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","115",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO115(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD115(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO115
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp115GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD115
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp115GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP115(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg115INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1115(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St115) bins
INSERT OVERWRITE TABLE taxish20150401_agg1115
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2115(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St115) bins
INSERT OVERWRITE TABLE taxish20150401_agg2115
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1115INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2115INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time120(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time120;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 12:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time120
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 12:30:00';

CREATE EXTERNAL TABLE taxish20150401_St120(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St120
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time120 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time120;
CREATE TABLE taxish20150401_Stmax120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin120(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St120 t GROUP BY t.carId) ts,taxish20150401_St120 t
INSERT OVERWRITE TABLE taxish20150401_Stmax120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St120 t GROUP BY t.carId) ts,taxish20150401_St120 t
INSERT OVERWRITE TABLE taxish20150401_Stmin120
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp120(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin120 tmin,taxish20150401_Stmax120 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp120
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf120(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf120
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp120 od1 JOIN taxish20150401_STODp120 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD120(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD120
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf120WHERE count>0;
drop table taxish20150401_Stmax120;
drop table taxish20150401_Stmin120;
drop table taxish20150401_STODf120;
FROM taxish20150401_STOD120INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO120(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD120(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO120
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp120GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD120
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp120GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP120(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP120INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","120",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO120(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD120(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO120
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp120GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD120
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp120GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP120(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg120INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1120(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St120) bins
INSERT OVERWRITE TABLE taxish20150401_agg1120
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2120(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St120) bins
INSERT OVERWRITE TABLE taxish20150401_agg2120
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1120INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2120INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time125(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time125;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 12:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time125
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 13:00:00';

CREATE EXTERNAL TABLE taxish20150401_St125(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St125
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time125 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time125;
CREATE TABLE taxish20150401_Stmax125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin125(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St125 t GROUP BY t.carId) ts,taxish20150401_St125 t
INSERT OVERWRITE TABLE taxish20150401_Stmax125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St125 t GROUP BY t.carId) ts,taxish20150401_St125 t
INSERT OVERWRITE TABLE taxish20150401_Stmin125
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp125(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin125 tmin,taxish20150401_Stmax125 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp125
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf125(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf125
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp125 od1 JOIN taxish20150401_STODp125 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD125(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD125
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf125WHERE count>0;
drop table taxish20150401_Stmax125;
drop table taxish20150401_Stmin125;
drop table taxish20150401_STODf125;
FROM taxish20150401_STOD125INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO125(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD125(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO125
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp125GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD125
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp125GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP125(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP125INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","125",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO125(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD125(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO125
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp125GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD125
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp125GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP125(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg125INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1125(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St125) bins
INSERT OVERWRITE TABLE taxish20150401_agg1125
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2125(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St125) bins
INSERT OVERWRITE TABLE taxish20150401_agg2125
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1125INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2125INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time130(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time130;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 13:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time130
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 13:30:00';

CREATE EXTERNAL TABLE taxish20150401_St130(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St130
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time130 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time130;
CREATE TABLE taxish20150401_Stmax130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin130(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St130 t GROUP BY t.carId) ts,taxish20150401_St130 t
INSERT OVERWRITE TABLE taxish20150401_Stmax130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St130 t GROUP BY t.carId) ts,taxish20150401_St130 t
INSERT OVERWRITE TABLE taxish20150401_Stmin130
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp130(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin130 tmin,taxish20150401_Stmax130 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp130
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf130(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf130
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp130 od1 JOIN taxish20150401_STODp130 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD130(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD130
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf130WHERE count>0;
drop table taxish20150401_Stmax130;
drop table taxish20150401_Stmin130;
drop table taxish20150401_STODf130;
FROM taxish20150401_STOD130INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO130(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD130(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO130
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp130GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD130
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp130GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP130(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP130INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","130",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO130(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD130(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO130
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp130GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD130
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp130GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP130(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg130INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1130(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St130) bins
INSERT OVERWRITE TABLE taxish20150401_agg1130
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2130(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St130) bins
INSERT OVERWRITE TABLE taxish20150401_agg2130
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1130INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2130INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time135(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time135;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 13:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time135
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 14:00:00';

CREATE EXTERNAL TABLE taxish20150401_St135(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St135
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time135 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time135;
CREATE TABLE taxish20150401_Stmax135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin135(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St135 t GROUP BY t.carId) ts,taxish20150401_St135 t
INSERT OVERWRITE TABLE taxish20150401_Stmax135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St135 t GROUP BY t.carId) ts,taxish20150401_St135 t
INSERT OVERWRITE TABLE taxish20150401_Stmin135
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp135(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin135 tmin,taxish20150401_Stmax135 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp135
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf135(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf135
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp135 od1 JOIN taxish20150401_STODp135 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD135(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD135
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf135WHERE count>0;
drop table taxish20150401_Stmax135;
drop table taxish20150401_Stmin135;
drop table taxish20150401_STODf135;
FROM taxish20150401_STOD135INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO135(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD135(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO135
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp135GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD135
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp135GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP135(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP135INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","135",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO135(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD135(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO135
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp135GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD135
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp135GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP135(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg135INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1135(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St135) bins
INSERT OVERWRITE TABLE taxish20150401_agg1135
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2135(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St135) bins
INSERT OVERWRITE TABLE taxish20150401_agg2135
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1135INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2135INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time140(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time140;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 14:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time140
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 14:30:00';

CREATE EXTERNAL TABLE taxish20150401_St140(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St140
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time140 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time140;
CREATE TABLE taxish20150401_Stmax140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin140(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St140 t GROUP BY t.carId) ts,taxish20150401_St140 t
INSERT OVERWRITE TABLE taxish20150401_Stmax140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St140 t GROUP BY t.carId) ts,taxish20150401_St140 t
INSERT OVERWRITE TABLE taxish20150401_Stmin140
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp140(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin140 tmin,taxish20150401_Stmax140 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp140
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf140(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf140
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp140 od1 JOIN taxish20150401_STODp140 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD140(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD140
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf140WHERE count>0;
drop table taxish20150401_Stmax140;
drop table taxish20150401_Stmin140;
drop table taxish20150401_STODf140;
FROM taxish20150401_STOD140INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO140(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD140(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO140
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp140GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD140
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp140GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP140(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP140INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","140",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO140(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD140(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO140
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp140GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD140
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp140GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP140(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg140INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1140(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St140) bins
INSERT OVERWRITE TABLE taxish20150401_agg1140
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2140(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St140) bins
INSERT OVERWRITE TABLE taxish20150401_agg2140
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1140INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2140INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time145(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time145;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 14:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time145
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 15:00:00';

CREATE EXTERNAL TABLE taxish20150401_St145(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St145
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time145 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time145;
CREATE TABLE taxish20150401_Stmax145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin145(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St145 t GROUP BY t.carId) ts,taxish20150401_St145 t
INSERT OVERWRITE TABLE taxish20150401_Stmax145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St145 t GROUP BY t.carId) ts,taxish20150401_St145 t
INSERT OVERWRITE TABLE taxish20150401_Stmin145
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp145(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin145 tmin,taxish20150401_Stmax145 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp145
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf145(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf145
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp145 od1 JOIN taxish20150401_STODp145 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD145(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD145
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf145WHERE count>0;
drop table taxish20150401_Stmax145;
drop table taxish20150401_Stmin145;
drop table taxish20150401_STODf145;
FROM taxish20150401_STOD145INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO145(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD145(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO145
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp145GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD145
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp145GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP145(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP145INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","145",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO145(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD145(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO145
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp145GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD145
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp145GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP145(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg145INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1145(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St145) bins
INSERT OVERWRITE TABLE taxish20150401_agg1145
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2145(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St145) bins
INSERT OVERWRITE TABLE taxish20150401_agg2145
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1145INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2145INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time150(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time150;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 15:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time150
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 15:30:00';

CREATE EXTERNAL TABLE taxish20150401_St150(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St150
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time150 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time150;
CREATE TABLE taxish20150401_Stmax150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin150(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St150 t GROUP BY t.carId) ts,taxish20150401_St150 t
INSERT OVERWRITE TABLE taxish20150401_Stmax150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St150 t GROUP BY t.carId) ts,taxish20150401_St150 t
INSERT OVERWRITE TABLE taxish20150401_Stmin150
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp150(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin150 tmin,taxish20150401_Stmax150 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp150
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf150(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf150
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp150 od1 JOIN taxish20150401_STODp150 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD150(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD150
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf150WHERE count>0;
drop table taxish20150401_Stmax150;
drop table taxish20150401_Stmin150;
drop table taxish20150401_STODf150;
FROM taxish20150401_STOD150INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO150(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD150(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO150
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp150GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD150
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp150GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP150(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP150INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","150",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO150(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD150(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO150
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp150GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD150
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp150GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP150(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg150INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1150(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St150) bins
INSERT OVERWRITE TABLE taxish20150401_agg1150
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2150(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St150) bins
INSERT OVERWRITE TABLE taxish20150401_agg2150
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1150INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2150INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time155(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time155;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 15:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time155
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 16:00:00';

CREATE EXTERNAL TABLE taxish20150401_St155(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St155
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time155 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time155;
CREATE TABLE taxish20150401_Stmax155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin155(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St155 t GROUP BY t.carId) ts,taxish20150401_St155 t
INSERT OVERWRITE TABLE taxish20150401_Stmax155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St155 t GROUP BY t.carId) ts,taxish20150401_St155 t
INSERT OVERWRITE TABLE taxish20150401_Stmin155
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp155(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin155 tmin,taxish20150401_Stmax155 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp155
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf155(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf155
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp155 od1 JOIN taxish20150401_STODp155 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD155(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD155
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf155WHERE count>0;
drop table taxish20150401_Stmax155;
drop table taxish20150401_Stmin155;
drop table taxish20150401_STODf155;
FROM taxish20150401_STOD155INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO155(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD155(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO155
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp155GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD155
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp155GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP155(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP155INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","155",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO155(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD155(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO155
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp155GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD155
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp155GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP155(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg155INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1155(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St155) bins
INSERT OVERWRITE TABLE taxish20150401_agg1155
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2155(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St155) bins
INSERT OVERWRITE TABLE taxish20150401_agg2155
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1155INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2155INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time160(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time160;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 16:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time160
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 16:30:00';

CREATE EXTERNAL TABLE taxish20150401_St160(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St160
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time160 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time160;
CREATE TABLE taxish20150401_Stmax160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin160(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St160 t GROUP BY t.carId) ts,taxish20150401_St160 t
INSERT OVERWRITE TABLE taxish20150401_Stmax160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St160 t GROUP BY t.carId) ts,taxish20150401_St160 t
INSERT OVERWRITE TABLE taxish20150401_Stmin160
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp160(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin160 tmin,taxish20150401_Stmax160 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp160
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf160(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf160
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp160 od1 JOIN taxish20150401_STODp160 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD160(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD160
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf160WHERE count>0;
drop table taxish20150401_Stmax160;
drop table taxish20150401_Stmin160;
drop table taxish20150401_STODf160;
FROM taxish20150401_STOD160INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO160(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD160(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO160
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp160GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD160
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp160GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP160(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP160INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","160",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO160(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD160(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO160
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp160GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD160
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp160GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP160(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg160INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1160(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St160) bins
INSERT OVERWRITE TABLE taxish20150401_agg1160
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2160(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St160) bins
INSERT OVERWRITE TABLE taxish20150401_agg2160
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1160INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2160INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time165(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time165;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 16:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time165
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 17:00:00';

CREATE EXTERNAL TABLE taxish20150401_St165(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St165
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time165 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time165;
CREATE TABLE taxish20150401_Stmax165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin165(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St165 t GROUP BY t.carId) ts,taxish20150401_St165 t
INSERT OVERWRITE TABLE taxish20150401_Stmax165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St165 t GROUP BY t.carId) ts,taxish20150401_St165 t
INSERT OVERWRITE TABLE taxish20150401_Stmin165
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp165(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin165 tmin,taxish20150401_Stmax165 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp165
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf165(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf165
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp165 od1 JOIN taxish20150401_STODp165 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD165(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD165
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf165WHERE count>0;
drop table taxish20150401_Stmax165;
drop table taxish20150401_Stmin165;
drop table taxish20150401_STODf165;
FROM taxish20150401_STOD165INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO165(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD165(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO165
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp165GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD165
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp165GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP165(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP165INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","165",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO165(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD165(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO165
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp165GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD165
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp165GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP165(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg165INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1165(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St165) bins
INSERT OVERWRITE TABLE taxish20150401_agg1165
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2165(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St165) bins
INSERT OVERWRITE TABLE taxish20150401_agg2165
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1165INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2165INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time170(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time170;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 17:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time170
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 17:30:00';

CREATE EXTERNAL TABLE taxish20150401_St170(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St170
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time170 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time170;
CREATE TABLE taxish20150401_Stmax170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin170(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St170 t GROUP BY t.carId) ts,taxish20150401_St170 t
INSERT OVERWRITE TABLE taxish20150401_Stmax170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St170 t GROUP BY t.carId) ts,taxish20150401_St170 t
INSERT OVERWRITE TABLE taxish20150401_Stmin170
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp170(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin170 tmin,taxish20150401_Stmax170 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp170
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf170(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf170
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp170 od1 JOIN taxish20150401_STODp170 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD170(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD170
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf170WHERE count>0;
drop table taxish20150401_Stmax170;
drop table taxish20150401_Stmin170;
drop table taxish20150401_STODf170;
FROM taxish20150401_STOD170INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO170(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD170(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO170
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp170GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD170
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp170GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP170(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP170INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","170",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO170(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD170(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO170
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp170GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD170
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp170GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP170(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg170INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1170(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St170) bins
INSERT OVERWRITE TABLE taxish20150401_agg1170
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2170(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St170) bins
INSERT OVERWRITE TABLE taxish20150401_agg2170
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1170INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2170INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time175(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time175;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 17:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time175
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 18:00:00';

CREATE EXTERNAL TABLE taxish20150401_St175(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St175
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time175 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time175;
CREATE TABLE taxish20150401_Stmax175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin175(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St175 t GROUP BY t.carId) ts,taxish20150401_St175 t
INSERT OVERWRITE TABLE taxish20150401_Stmax175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St175 t GROUP BY t.carId) ts,taxish20150401_St175 t
INSERT OVERWRITE TABLE taxish20150401_Stmin175
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp175(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin175 tmin,taxish20150401_Stmax175 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp175
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf175(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf175
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp175 od1 JOIN taxish20150401_STODp175 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD175(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD175
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf175WHERE count>0;
drop table taxish20150401_Stmax175;
drop table taxish20150401_Stmin175;
drop table taxish20150401_STODf175;
FROM taxish20150401_STOD175INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO175(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD175(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO175
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp175GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD175
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp175GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP175(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP175INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","175",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO175(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD175(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO175
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp175GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD175
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp175GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP175(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg175INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1175(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St175) bins
INSERT OVERWRITE TABLE taxish20150401_agg1175
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2175(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St175) bins
INSERT OVERWRITE TABLE taxish20150401_agg2175
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1175INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2175INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time180(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time180;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 18:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time180
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 18:30:00';

CREATE EXTERNAL TABLE taxish20150401_St180(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St180
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time180 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time180;
CREATE TABLE taxish20150401_Stmax180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin180(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St180 t GROUP BY t.carId) ts,taxish20150401_St180 t
INSERT OVERWRITE TABLE taxish20150401_Stmax180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St180 t GROUP BY t.carId) ts,taxish20150401_St180 t
INSERT OVERWRITE TABLE taxish20150401_Stmin180
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp180(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin180 tmin,taxish20150401_Stmax180 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp180
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf180(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf180
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp180 od1 JOIN taxish20150401_STODp180 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD180(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD180
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf180WHERE count>0;
drop table taxish20150401_Stmax180;
drop table taxish20150401_Stmin180;
drop table taxish20150401_STODf180;
FROM taxish20150401_STOD180INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO180(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD180(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO180
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp180GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD180
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp180GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP180(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP180INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","180",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO180(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD180(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO180
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp180GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD180
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp180GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP180(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg180INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1180(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St180) bins
INSERT OVERWRITE TABLE taxish20150401_agg1180
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2180(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St180) bins
INSERT OVERWRITE TABLE taxish20150401_agg2180
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1180INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2180INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time185(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time185;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 18:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time185
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 19:00:00';

CREATE EXTERNAL TABLE taxish20150401_St185(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St185
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time185 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time185;
CREATE TABLE taxish20150401_Stmax185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmax185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmin185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin185 tmin,taxish20150401_Stmax185 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp185
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf185
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp185 od1 JOIN taxish20150401_STODp185 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD185(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD185
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf185WHERE count>0;
drop table taxish20150401_Stmax185;
drop table taxish20150401_Stmin185;
drop table taxish20150401_STODf185;
FROM taxish20150401_STOD185INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO185(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD185(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO185
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp185GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD185
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp185GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP185(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP185INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","185",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO185(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD185(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO185
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp185GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD185
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp185GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP185(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg185INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1185(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St185) bins
INSERT OVERWRITE TABLE taxish20150401_agg1185
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2185(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St185) bins
INSERT OVERWRITE TABLE taxish20150401_agg2185
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1185INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2185INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time190(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time190;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 19:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time190
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 19:30:00';

CREATE EXTERNAL TABLE taxish20150401_St190(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St190
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time190 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time190;
CREATE TABLE taxish20150401_Stmax190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin190(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St190 t GROUP BY t.carId) ts,taxish20150401_St190 t
INSERT OVERWRITE TABLE taxish20150401_Stmax190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St190 t GROUP BY t.carId) ts,taxish20150401_St190 t
INSERT OVERWRITE TABLE taxish20150401_Stmin190
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp190(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin190 tmin,taxish20150401_Stmax190 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp190
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf190(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf190
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp190 od1 JOIN taxish20150401_STODp190 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD190(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD190
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf190WHERE count>0;
drop table taxish20150401_Stmax190;
drop table taxish20150401_Stmin190;
drop table taxish20150401_STODf190;
FROM taxish20150401_STOD190INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO190(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD190(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO190
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp190GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD190
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp190GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP190(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP190INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","190",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO190(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD190(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO190
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp190GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD190
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp190GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP190(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg190INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1190(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St190) bins
INSERT OVERWRITE TABLE taxish20150401_agg1190
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2190(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St190) bins
INSERT OVERWRITE TABLE taxish20150401_agg2190
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1190INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2190INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time195(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time195;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 19:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time195
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 20:00:00';

CREATE EXTERNAL TABLE taxish20150401_St195(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St195
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time195 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time195;
CREATE TABLE taxish20150401_Stmax195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin195(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St195 t GROUP BY t.carId) ts,taxish20150401_St195 t
INSERT OVERWRITE TABLE taxish20150401_Stmax195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St195 t GROUP BY t.carId) ts,taxish20150401_St195 t
INSERT OVERWRITE TABLE taxish20150401_Stmin195
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp195(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin195 tmin,taxish20150401_Stmax195 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp195
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf195(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf195
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp195 od1 JOIN taxish20150401_STODp195 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD195(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD195
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf195WHERE count>0;
drop table taxish20150401_Stmax195;
drop table taxish20150401_Stmin195;
drop table taxish20150401_STODf195;
FROM taxish20150401_STOD195INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO195(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD195(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO195
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp195GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD195
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp195GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP195(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP195INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","195",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO195(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD195(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO195
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp195GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD195
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp195GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP195(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg195INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1195(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St195) bins
INSERT OVERWRITE TABLE taxish20150401_agg1195
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2195(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St195) bins
INSERT OVERWRITE TABLE taxish20150401_agg2195
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1195INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2195INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time200(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time200;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 20:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time200
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 20:30:00';

CREATE EXTERNAL TABLE taxish20150401_St200(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St200
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time200 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time200;
CREATE TABLE taxish20150401_Stmax200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin200(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St200 t GROUP BY t.carId) ts,taxish20150401_St200 t
INSERT OVERWRITE TABLE taxish20150401_Stmax200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St200 t GROUP BY t.carId) ts,taxish20150401_St200 t
INSERT OVERWRITE TABLE taxish20150401_Stmin200
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp200(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin200 tmin,taxish20150401_Stmax200 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp200
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf200(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf200
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp200 od1 JOIN taxish20150401_STODp200 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD200(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD200
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf200WHERE count>0;
drop table taxish20150401_Stmax200;
drop table taxish20150401_Stmin200;
drop table taxish20150401_STODf200;
FROM taxish20150401_STOD200INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO200(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD200(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO200
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp200GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD200
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp200GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP200(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP200INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","200",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO200(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD200(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO200
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp200GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD200
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp200GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP200(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg200INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1200(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St200) bins
INSERT OVERWRITE TABLE taxish20150401_agg1200
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2200(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St200) bins
INSERT OVERWRITE TABLE taxish20150401_agg2200
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1200INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2200INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time205(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time205;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 20:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time205
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 21:00:00';

CREATE EXTERNAL TABLE taxish20150401_St205(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St205
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time205 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time205;
CREATE TABLE taxish20150401_Stmax205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin205(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St205 t GROUP BY t.carId) ts,taxish20150401_St205 t
INSERT OVERWRITE TABLE taxish20150401_Stmax205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St205 t GROUP BY t.carId) ts,taxish20150401_St205 t
INSERT OVERWRITE TABLE taxish20150401_Stmin205
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp205(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin205 tmin,taxish20150401_Stmax205 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp205
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf205(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf205
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp205 od1 JOIN taxish20150401_STODp205 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD205(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD205
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf205WHERE count>0;
drop table taxish20150401_Stmax205;
drop table taxish20150401_Stmin205;
drop table taxish20150401_STODf205;
FROM taxish20150401_STOD205INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO205(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD205(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO205
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp205GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD205
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp205GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP205(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP205INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","205",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO205(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD205(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO205
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp205GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD205
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp205GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP205(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg205INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1205(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St205) bins
INSERT OVERWRITE TABLE taxish20150401_agg1205
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2205(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St205) bins
INSERT OVERWRITE TABLE taxish20150401_agg2205
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1205INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2205INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time210(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time210;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 21:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time210
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 21:30:00';

CREATE EXTERNAL TABLE taxish20150401_St210(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St210
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time210 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time210;
CREATE TABLE taxish20150401_Stmax210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin210(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St210 t GROUP BY t.carId) ts,taxish20150401_St210 t
INSERT OVERWRITE TABLE taxish20150401_Stmax210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St210 t GROUP BY t.carId) ts,taxish20150401_St210 t
INSERT OVERWRITE TABLE taxish20150401_Stmin210
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp210(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin210 tmin,taxish20150401_Stmax210 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp210
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf210(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf210
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp210 od1 JOIN taxish20150401_STODp210 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD210(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD210
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf210WHERE count>0;
drop table taxish20150401_Stmax210;
drop table taxish20150401_Stmin210;
drop table taxish20150401_STODf210;
FROM taxish20150401_STOD210INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO210(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD210(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO210
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp210GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD210
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp210GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP210(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP210INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","210",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO210(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD210(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO210
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp210GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD210
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp210GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP210(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg210INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1210(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St210) bins
INSERT OVERWRITE TABLE taxish20150401_agg1210
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2210(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St210) bins
INSERT OVERWRITE TABLE taxish20150401_agg2210
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1210INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2210INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time215(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time215;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 21:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time215
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 22:00:00';

CREATE EXTERNAL TABLE taxish20150401_St215(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St215
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time215 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time215;
CREATE TABLE taxish20150401_Stmax215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin215(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St215 t GROUP BY t.carId) ts,taxish20150401_St215 t
INSERT OVERWRITE TABLE taxish20150401_Stmax215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St215 t GROUP BY t.carId) ts,taxish20150401_St215 t
INSERT OVERWRITE TABLE taxish20150401_Stmin215
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp215(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin215 tmin,taxish20150401_Stmax215 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp215
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf215(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf215
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp215 od1 JOIN taxish20150401_STODp215 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD215(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD215
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf215WHERE count>0;
drop table taxish20150401_Stmax215;
drop table taxish20150401_Stmin215;
drop table taxish20150401_STODf215;
FROM taxish20150401_STOD215INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO215(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD215(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO215
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp215GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD215
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp215GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP215(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP215INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","215",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO215(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD215(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO215
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp215GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD215
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp215GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP215(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg215INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1215(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St215) bins
INSERT OVERWRITE TABLE taxish20150401_agg1215
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2215(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St215) bins
INSERT OVERWRITE TABLE taxish20150401_agg2215
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1215INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2215INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time220(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time220;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 22:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time220
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 22:30:00';

CREATE EXTERNAL TABLE taxish20150401_St220(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St220
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time220 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time220;
CREATE TABLE taxish20150401_Stmax220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin220(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St220 t GROUP BY t.carId) ts,taxish20150401_St220 t
INSERT OVERWRITE TABLE taxish20150401_Stmax220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St220 t GROUP BY t.carId) ts,taxish20150401_St220 t
INSERT OVERWRITE TABLE taxish20150401_Stmin220
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp220(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin220 tmin,taxish20150401_Stmax220 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp220
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf220(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf220
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp220 od1 JOIN taxish20150401_STODp220 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD220(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD220
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf220WHERE count>0;
drop table taxish20150401_Stmax220;
drop table taxish20150401_Stmin220;
drop table taxish20150401_STODf220;
FROM taxish20150401_STOD220INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO220(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD220(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO220
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp220GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD220
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp220GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP220(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP220INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","220",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO220(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD220(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO220
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp220GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD220
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp220GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP220(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg220INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1220(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St220) bins
INSERT OVERWRITE TABLE taxish20150401_agg1220
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2220(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St220) bins
INSERT OVERWRITE TABLE taxish20150401_agg2220
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1220INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2220INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time225(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time225;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 22:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time225
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 23:00:00';

CREATE EXTERNAL TABLE taxish20150401_St225(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St225
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time225 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time225;
CREATE TABLE taxish20150401_Stmax225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin225(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St225 t GROUP BY t.carId) ts,taxish20150401_St225 t
INSERT OVERWRITE TABLE taxish20150401_Stmax225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St225 t GROUP BY t.carId) ts,taxish20150401_St225 t
INSERT OVERWRITE TABLE taxish20150401_Stmin225
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp225(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin225 tmin,taxish20150401_Stmax225 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp225
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf225(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf225
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp225 od1 JOIN taxish20150401_STODp225 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD225(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD225
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf225WHERE count>0;
drop table taxish20150401_Stmax225;
drop table taxish20150401_Stmin225;
drop table taxish20150401_STODf225;
FROM taxish20150401_STOD225INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO225(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD225(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO225
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp225GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD225
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp225GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP225(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP225INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","225",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO225(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD225(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO225
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp225GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD225
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp225GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP225(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg225INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1225(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St225) bins
INSERT OVERWRITE TABLE taxish20150401_agg1225
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2225(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St225) bins
INSERT OVERWRITE TABLE taxish20150401_agg2225
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1225INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2225INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time230(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time230;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 23:00:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time230
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 23:30:00';

CREATE EXTERNAL TABLE taxish20150401_St230(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St230
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time230 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time230;
CREATE TABLE taxish20150401_Stmax230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin230(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St230 t GROUP BY t.carId) ts,taxish20150401_St230 t
INSERT OVERWRITE TABLE taxish20150401_Stmax230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St230 t GROUP BY t.carId) ts,taxish20150401_St230 t
INSERT OVERWRITE TABLE taxish20150401_Stmin230
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp230(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin230 tmin,taxish20150401_Stmax230 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp230
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf230(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf230
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp230 od1 JOIN taxish20150401_STODp230 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD230(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD230
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf230WHERE count>0;
drop table taxish20150401_Stmax230;
drop table taxish20150401_Stmin230;
drop table taxish20150401_STODf230;
FROM taxish20150401_STOD230INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO230(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD230(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO230
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp230GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD230
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp230GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP230(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP230INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","230",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO230(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD230(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO230
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp230GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD230
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp230GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP230(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg230INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1230(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St230) bins
INSERT OVERWRITE TABLE taxish20150401_agg1230
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2230(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St230) bins
INSERT OVERWRITE TABLE taxish20150401_agg2230
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1230INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2230INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_time235(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
describe taxish20150401_time235;
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_.receiveTime > '2015-04-01 23:30:00') taxish150401s
INSERT OVERWRITE TABLE taxish20150401_time235
SELECT *
WHERE taxish150401s.receiveTime < '2015-04-01 24:00:00';

CREATE EXTERNAL TABLE taxish20150401_St235(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_St235
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time235 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
drop table taxish20150401_time235;
CREATE TABLE taxish20150401_Stmax235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
CREATE TABLE taxish20150401_Stmin235(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St235 t GROUP BY t.carId) ts,taxish20150401_St235 t
INSERT OVERWRITE TABLE taxish20150401_Stmax235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St235 t GROUP BY t.carId) ts,taxish20150401_St235 t
INSERT OVERWRITE TABLE taxish20150401_Stmin235
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
CREATE TABLE taxish20150401_STODp235(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin235 tmin,taxish20150401_Stmax235 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp235
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
CREATE TABLE taxish20150401_STODf235(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STODf235
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp235 od1 JOIN taxish20150401_STODp235 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
CREATE TABLE taxish20150401_STOD235(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
INSERT OVERWRITE TABLE taxish20150401_STOD235
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf235WHERE count>0;
drop table taxish20150401_Stmax235;
drop table taxish20150401_Stmin235;
drop table taxish20150401_STODf235;
FROM taxish20150401_STOD235INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(count),MAX(count);

CREATE TABLE taxish20150401_STO235(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD235(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO235
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp235GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD235
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp235GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP235(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_STTP235INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","235",MIN(tpcount),MAX(tpcount);

CREATE TABLE taxish20150401_STO235(OctOBJECTID int,count DOUBLE);
CREATE TABLE taxish20150401_STD235(DctOBJECTID int,count DOUBLE);
INSERT OVERWRITE TABLE taxish20150401_STO235
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp235GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD235
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp235GROUP BY DctOBJECTID,Dctcx,Dctcy;
CREATE TABLE taxish20150401_STTP235(area BINARY,tpcount DOUBLE)
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
FROM taxish20150401_stagg235INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG",""+time+"",MIN(stcount),MAX(stcount);

CREATE TABLE taxish20150401_agg1235(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St235) bins
INSERT OVERWRITE TABLE taxish20150401_agg1235
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
CREATE TABLE taxish20150401_agg2235(area BINARY, count DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St235) bins
INSERT OVERWRITE TABLE taxish20150401_agg2235
SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count
GROUP BY bin_id;

FROM taxish20150401_agg1235INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(count),MAX(count);
FROM taxish20150401_agg2235INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(count),MAX(count);

INSERT INTO TABLE taxish20150401_value 
SELECT * FROM taxish20150401_valuep;
drop table taxish20150401_valuep;
