#加载用户自定义函数jar包，路径为HDFS中的路径
add jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar
    /root/esri-git/json-serde-1.3.8-jar-with-dependencies.jar
    /root/esri-git/json-udf-1.3.8-jar-with-dependencies.jar;
#加载后续分析使用的临时函数
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';

#pre
#block

#创建行政区划数据表，字段与源文件相同，使用外部表保证数据安全，Esri的Json序列化模式，封闭式文本格式
DROP TABLE IF EXISTS blocksh_v1p;
CREATE EXTERNAL TABLE blocksh_v1p (Name string, objectid string, cx DOUBLE,cy DOUBLE,BoundaryShape binary)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
#装载HDFS中的上海街道区划数据，实质是HDFS中的文件移动
LOAD DATA INPATH  '/SHstreet/newEnCounty.json' OVERWRITE INTO TABLE blocksh_v1p;

describe blocksh_v1p;

#taxish20150401将时间做成时间戳存储
#hadoop fs -put /home/part-4.01.csv /taxidemo

#创建轨迹数据表，字段与源文件相同，使用外部表保证数据安全，以“,”作为分隔符，文本格式，第一行表头忽略
DROP TABLE IF EXISTS taxish20150401_;
CREATE EXTERNAL TABLE taxish20150401_(carId DOUBLE,isAlarm DOUBLE,isEmpty DOUBLE,topLight DOUBLE,
Elevated DOUBLE,isBrake DOUBLE,receiveTime TIMESTAMP,GPSTime STRING,longitude DOUBLE,latitude DOUBLE,
speed DOUBLE,direction DOUBLE,satellite DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile
tblproperties ("skip.header.line.count"="1");
#装载HDFS中的上海出租车数据，以20150401数据为例，实质是HDFS中的文件移动
LOAD DATA INPATH '/taxidemo/part-4.01.csv' OVERWRITE INTO TABLE taxish20150401_;

describe taxish20150401_timecn;

#最大最小值存储
DROP TABLE IF EXISTS taxish20150401_value;
CREATE EXTERNAL TABLE taxish20150401_value(p STRING,m STRING,n DOUBLE,x DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;

DROP TABLE IF EXISTS taxish20150401_valuep;
CREATE TABLE taxish20150401_valuep(type STRING,time STRING,min DOUBLE,max DOUBLE);

#Day divide
#stod 2+1+1+235+1+911+1+44+45+14+13+3+3=1274
#sttp 2+2+40+7=51
#stagg 248+7=255
#agg 8+8+3+3=22
#all 1602s=26.7min
#time

#创建半小时数据表存储轨迹数据，保留出租车ID和时空数据，185表示18:30~19:00
DROP TABLE IF EXISTS taxish20150401_time185;
CREATE TABLE taxish20150401_time185(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);
#使用时间戳的比较内置函数进行时间分割
FROM (SELECT carId,receiveTime,longitude,latitude FROM taxish20150401_ WHERE taxish20150401_timecn.receiveTime > '2015-04-01 18:30:00') taxish20150401s
INSERT OVERWRITE TABLE taxish20150401_time185
SELECT *
WHERE taxish20150401s.receiveTime < '2015-04-01 19:00:00';
#运行数据大约220s，结果数据条数为2百万左右

#创建中间值表存储包含轨迹和其所在区划的轨迹街区数据表，也为后文使用准备
DROP TABLE IF EXISTS taxish20150401_St185;
CREATE TABLE taxish20150401_St185(carId DOUBLE,receiveTime string,longitude DOUBLE,
	latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);
#从轨迹数据上海出租车表和区划街区数据表一起输入数据
#187个街区，2百万条出租车记录
#WHERE语句使用ST_Contains判断出租车点(用ST_Point构建)所在的街区
#当判断结果为真，写入轨迹街区数据表原出租车的时空数据和其所在街区的名字、ID和中心点
INSERT OVERWRITE TABLE taxish20150401_St185
SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME,bp.OBJECTID,bp.cx,bp.cy
FROM blocksh_v1p bp JOIN taxish20150401_time185 tt
WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));
#某次运行花费768秒，包含ST_Contains函数计算，不包含统计

#agg栅格热力*********************************************************************************************************************************************
#以185:18:30~19:00为例，原数据量为2百万条左右
#创建存储栅格聚合统计的表，含有栅格多边形（正方形）所以使用Esri的Json格式，字段属性BINARY
#agg1意义为Bin Size为0.01，agg2意义为Bin Size为0.02
DROP TABLE IF EXISTS taxish20150401_agg1185;
CREATE TABLE taxish20150401_agg1185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
#从原数据的时间分割片中选择栅格ID和所有数据
#使用COUNT（*）函数，以BIN ID为分组，每行一个格，统计分割后每个格中的点数量
#返回栅格每个格子的边界点集，和格内统计数量，便于绘制栅格聚合统计热点图
FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM taxish20150401_St185) bins
INSERT OVERWRITE TABLE taxish20150401_agg1185
SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count
GROUP BY bin_id;
#某次统计运行时间为：8.231秒，Bin Size为0.02时会更快
#对于全天1.15亿的数据，以Bin Size为0.001的运行时间为305秒，以Bin Size为0.05的运行时间为274秒
FROM taxish20150401_agg1185
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG1","185",MIN(c),MAX(c);
#2.548 seconds
FROM taxish20150401_agg2185
INSERT INTO TABLE taxish20150401_valuep
SELECT "AGG2","185",MIN(c),MAX(c);
#2.551 seconds


#stagg街区热力**************************************************************************************************************************************
#创建街区关联聚合统计表stagg，返回值包括街区边界，使用Esri的Json格式，字段属性BINARY
DROP TABLE IF EXISTS taxish20150401_stagg185;
CREATE TABLE taxish20150401_stagg185(area BINARY, c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
#从前文的轨迹街区数据表和区划数据表输入数据
#187个街区，2百万条出租车记录
#判断出租车所在的街区ID和街区ID相同时
#使用COUNT（*）函数，以街区为分组，每行一个街区，统计其中的轨迹点数量
#写入关联聚合统计表每个街区边界点集合和其中规矩点的统计值
INSERT OVERWRITE TABLE taxish20150401_stagg185
SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St185 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.BoundaryShape;
#某次运行花费212秒，不包含ST_Contains函数计算，只是统计
FROM taxish20150401_stagg185
INSERT INTO TABLE taxish20150401_valuep
SELECT "STAGG","185",MIN(c),MAX(c);
#4.546 seconds

#stod街区OD*****************************************************************************************************************************************************8
#轨迹OD调查分析
#首先根据对每一辆车在半小时中的位置变化计算这个时间段出租车的OD，并拟合街区中心点作为坐标
#创建Stmax、Stmin数据表分别存储时间段内一辆车的时间最大最小值，和其所在的街区ID和中心点
DROP TABLE IF EXISTS taxish20150401_Stmax185;
CREATE TABLE taxish20150401_Stmax185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);
DROP TABLE IF EXISTS taxish20150401_Stmin185;
CREATE TABLE taxish20150401_Stmin185(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);
#从上文计算的轨迹街区数据表输入
#distinct去除重复数据，舍掉轨迹自身的坐标，以所在街区中心点坐标替代
#使用unix_timestamp()将时间戳转化为数值，使用MAX()和MIN()函数，以车的ID分组，计算每一辆车时间戳的最大和最小值
#和原轨迹数据比对车ID和时间大小，写入Stmax、Stmin表车辆ID，时间段内该辆车的时间最大最小值和其所在的街区ID和中心点
FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmax185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max
WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);
FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM taxish20150401_St185 t GROUP BY t.carId) ts,taxish20150401_St185 t
INSERT OVERWRITE TABLE taxish20150401_Stmin185
SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min
WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);
#运行时间分别为40秒左右

#创建STODp表存储街区的轨迹OD最初结果，除了起讫点的街区ID和中心坐标值，还有对相同起讫点的轨迹统计量count，为正值
DROP TABLE IF EXISTS taxish20150401_STODp185;
CREATE TABLE taxish20150401_STODp185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);
#分别从Stmax、Stmin表中，对相同的车ID判断起点和终点街区
#使用COUNT（*）函数，以轨迹OD为分组，每行一个轨迹街区OD，统计相同起止街区轨迹OD的数量
#写入STODp统计表每个不同轨迹OD的起止街区ID和中心点，以及每一个相同轨迹OD的统计量
FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM taxish20150401_Stmin185 tmin,taxish20150401_Stmax185 tmax WHERE tmin.carId=tmax.carId) tod
INSERT OVERWRITE TABLE taxish20150401_STODp185
SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count
GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;
#运行时间14s

#创建STODf表存储街+区的轨迹OD中间结果，与最初结果字段相同，但由于起止点相反的轨迹没有计算，合并后count可正可负
DROP TABLE IF EXISTS taxish20150401_STODf185;
CREATE TABLE taxish20150401_STODf185(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,
	Dctcy DOUBLE,count DOUBLE);
#从轨迹OD最初结果表STODp输入两次数据
#当两条记录起止点正好相反时，即你的起点O是我的终点D，你的终点D是我的起点O，将两条记录的统计量count做差
#写入STODf统计表每个不同轨迹OD的起止街区ID和中心点，以及每一个相同轨迹OD的净统计量
INSERT OVERWRITE TABLE taxish20150401_STODf185
SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count
FROM taxish20150401_STODp185 od1 JOIN taxish20150401_STODp185 od2
WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;
#运行时间113s

#创建STOD数据表存储最终的起讫点数据，包括起止中心坐标值，还有对相同起讫点的轨迹统计量count
#使用正常的Json格式存储，去掉起止街区ID，加快后续数据传输
DROP TABLE IF EXISTS taxish20150401_STOD185;
CREATE TABLE taxish20150401_STOD185(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS textfile;
#上一步计算中对互为起止点的轨迹统计量做差，当两条记录互为起止点时，统计量为相反数
#判断count>0时，存储轨迹记录的起讫点和统计量写入STOD最终数据表
INSERT OVERWRITE TABLE taxish20150401_STOD185
SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM taxish20150401_STODf185
WHERE count>0;

drop table taxish20150401_Stmax185;
drop table taxish20150401_Stmin185;
drop table taxish20150401_STODf185;
FROM taxish20150401_STOD185
INSERT INTO TABLE taxish20150401_valuep
SELECT "STOD","185",MIN(c),MAX(c);
#2.565 seconds

#sttp街区吞吐量分析********************************************************************************************************************************
#创建街区起/讫统计表存储街区的起点STO和终点STD统计，包括街区ID和统计量
DROP TABLE IF EXISTS taxish20150401_STO185;
CREATE TABLE taxish20150401_STO185(OctOBJECTID int,count DOUBLE);
DROP TABLE IF EXISTS taxish20150401_STD185;
CREATE TABLE taxish20150401_STD185(DctOBJECTID int,count DOUBLE);
#输入为上一步中STODp，没有处理互为起讫点的轨迹原始OD数据
#使用SUM（*）函数，以街区起点为分组，统计一个街区驶出/驶入出租车的数量
#存储街区ID和驶出/驶入统计值写入街区起/讫统计表
INSERT OVERWRITE TABLE taxish20150401_STO185
SELECT OctOBJECTID,SUM(count)
FROM taxish20150401_STODp185
GROUP BY OctOBJECTID,Octcx,Octcy;
INSERT OVERWRITE TABLE taxish20150401_STD185
SELECT DctOBJECTID,SUM(count)
FROM taxish20150401_STODp185
GROUP BY DctOBJECTID,Dctcx,Dctcy;
#运行时间分别为2秒左右
#创建街区吞吐量统计表sttp，返回值包括街区边界，使用Esri的Json格式，字段属性BINARY
DROP TABLE IF EXISTS taxish20150401_STTP185;
CREATE TABLE taxish20150401_STTP185(area BINARY,c DOUBLE)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
#输入数据为街区起和讫统计表和街区表
#判断起讫街区相同，计算起讫统计量的差值为净吞吐量
#写入街区吞吐量统计表街区的边界点集和净吞吐量
FROM taxish20150401_STO185 o,taxish20150401_STD185 d, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTP185
SELECT distinct b.BoundaryShape,o.count-d.count
WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;
#运行时间为30秒左右
drop table taxish20150401_STO185;
drop table taxish20150401_STD185;
drop table taxish20150401_STODp185;
FROM taxish20150401_STTP185
INSERT INTO TABLE taxish20150401_valuep
SELECT "STTP","185",MIN(c),MAX(c);
#4.562 seconds

