add jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar
    /root/esri-git/json-serde-1.3.8-jar-with-dependencies.jar
    /root/esri-git/json-udf-1.3.8-jar-with-dependencies.jar;
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';

DROP TABLE IF EXISTS taxish20150401_STTPn000;
CREATE TABLE taxish20150401_STTPn000(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP000 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn000
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn000;
CREATE TABLE taxish20150401_staggn000(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn000
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St000 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn005;
CREATE TABLE taxish20150401_STTPn005(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP005 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn005
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn005;
CREATE TABLE taxish20150401_staggn005(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn005
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St005 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn010;
CREATE TABLE taxish20150401_STTPn010(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP010 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn010
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn010;
CREATE TABLE taxish20150401_staggn010(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn010
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St010 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn015;
CREATE TABLE taxish20150401_STTPn015(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP015 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn015
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn015;
CREATE TABLE taxish20150401_staggn015(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn015
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St015 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn020;
CREATE TABLE taxish20150401_STTPn020(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP020 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn020
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn020;
CREATE TABLE taxish20150401_staggn020(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn020
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St020 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn025;
CREATE TABLE taxish20150401_STTPn025(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP025 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn025
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn025;
CREATE TABLE taxish20150401_staggn025(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn025
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St025 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn030;
CREATE TABLE taxish20150401_STTPn030(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP030 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn030
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn030;
CREATE TABLE taxish20150401_staggn030(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn030
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St030 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn035;
CREATE TABLE taxish20150401_STTPn035(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP035 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn035
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn035;
CREATE TABLE taxish20150401_staggn035(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn035
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St035 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn040;
CREATE TABLE taxish20150401_STTPn040(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP040 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn040
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn040;
CREATE TABLE taxish20150401_staggn040(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn040
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St040 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn045;
CREATE TABLE taxish20150401_STTPn045(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP045 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn045
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn045;
CREATE TABLE taxish20150401_staggn045(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn045
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St045 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn050;
CREATE TABLE taxish20150401_STTPn050(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP050 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn050
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn050;
CREATE TABLE taxish20150401_staggn050(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn050
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St050 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn055;
CREATE TABLE taxish20150401_STTPn055(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP055 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn055
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn055;
CREATE TABLE taxish20150401_staggn055(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn055
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St055 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn060;
CREATE TABLE taxish20150401_STTPn060(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP060 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn060
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn060;
CREATE TABLE taxish20150401_staggn060(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn060
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St060 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn065;
CREATE TABLE taxish20150401_STTPn065(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP065 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn065
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn065;
CREATE TABLE taxish20150401_staggn065(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn065
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St065 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn070;
CREATE TABLE taxish20150401_STTPn070(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP070 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn070
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn070;
CREATE TABLE taxish20150401_staggn070(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn070
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St070 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn075;
CREATE TABLE taxish20150401_STTPn075(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP075 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn075
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn075;
CREATE TABLE taxish20150401_staggn075(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn075
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St075 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn080;
CREATE TABLE taxish20150401_STTPn080(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP080 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn080
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn080;
CREATE TABLE taxish20150401_staggn080(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn080
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St080 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn085;
CREATE TABLE taxish20150401_STTPn085(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP085 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn085
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn085;
CREATE TABLE taxish20150401_staggn085(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn085
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St085 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn090;
CREATE TABLE taxish20150401_STTPn090(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP090 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn090
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn090;
CREATE TABLE taxish20150401_staggn090(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn090
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St090 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn095;
CREATE TABLE taxish20150401_STTPn095(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP095 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn095
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn095;
CREATE TABLE taxish20150401_staggn095(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn095
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St095 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn100;
CREATE TABLE taxish20150401_STTPn100(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP100 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn100
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn100;
CREATE TABLE taxish20150401_staggn100(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn100
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St100 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn105;
CREATE TABLE taxish20150401_STTPn105(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP105 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn105
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn105;
CREATE TABLE taxish20150401_staggn105(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn105
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St105 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn110;
CREATE TABLE taxish20150401_STTPn110(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP110 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn110
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn110;
CREATE TABLE taxish20150401_staggn110(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn110
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St110 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn115;
CREATE TABLE taxish20150401_STTPn115(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP115 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn115
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn115;
CREATE TABLE taxish20150401_staggn115(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn115
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St115 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn120;
CREATE TABLE taxish20150401_STTPn120(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP120 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn120
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn120;
CREATE TABLE taxish20150401_staggn120(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn120
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St120 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn125;
CREATE TABLE taxish20150401_STTPn125(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP125 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn125
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn125;
CREATE TABLE taxish20150401_staggn125(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn125
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St125 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn130;
CREATE TABLE taxish20150401_STTPn130(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP130 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn130
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn130;
CREATE TABLE taxish20150401_staggn130(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn130
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St130 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn135;
CREATE TABLE taxish20150401_STTPn135(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP135 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn135
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn135;
CREATE TABLE taxish20150401_staggn135(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn135
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St135 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn140;
CREATE TABLE taxish20150401_STTPn140(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP140 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn140
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn140;
CREATE TABLE taxish20150401_staggn140(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn140
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St140 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn145;
CREATE TABLE taxish20150401_STTPn145(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP145 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn145
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn145;
CREATE TABLE taxish20150401_staggn145(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn145
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St145 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn150;
CREATE TABLE taxish20150401_STTPn150(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP150 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn150
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn150;
CREATE TABLE taxish20150401_staggn150(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn150
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St150 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn155;
CREATE TABLE taxish20150401_STTPn155(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP155 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn155
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn155;
CREATE TABLE taxish20150401_staggn155(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn155
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St155 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn160;
CREATE TABLE taxish20150401_STTPn160(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP160 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn160
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn160;
CREATE TABLE taxish20150401_staggn160(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn160
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St160 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn165;
CREATE TABLE taxish20150401_STTPn165(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP165 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn165
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn165;
CREATE TABLE taxish20150401_staggn165(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn165
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St165 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn170;
CREATE TABLE taxish20150401_STTPn170(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP170 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn170
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn170;
CREATE TABLE taxish20150401_staggn170(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn170
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St170 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn175;
CREATE TABLE taxish20150401_STTPn175(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP175 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn175
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn175;
CREATE TABLE taxish20150401_staggn175(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn175
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St175 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn180;
CREATE TABLE taxish20150401_STTPn180(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP180 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn180
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn180;
CREATE TABLE taxish20150401_staggn180(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn180
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St180 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn185;
CREATE TABLE taxish20150401_STTPn185(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP185 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn185
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn185;
CREATE TABLE taxish20150401_staggn185(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn185
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St185 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn190;
CREATE TABLE taxish20150401_STTPn190(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP190 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn190
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn190;
CREATE TABLE taxish20150401_staggn190(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn190
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St190 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn195;
CREATE TABLE taxish20150401_STTPn195(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP195 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn195
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn195;
CREATE TABLE taxish20150401_staggn195(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn195
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St195 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn200;
CREATE TABLE taxish20150401_STTPn200(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP200 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn200
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn200;
CREATE TABLE taxish20150401_staggn200(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn200
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St200 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn205;
CREATE TABLE taxish20150401_STTPn205(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP205 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn205
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn205;
CREATE TABLE taxish20150401_staggn205(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn205
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St205 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn210;
CREATE TABLE taxish20150401_STTPn210(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP210 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn210
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn210;
CREATE TABLE taxish20150401_staggn210(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn210
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St210 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn215;
CREATE TABLE taxish20150401_STTPn215(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP215 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn215
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn215;
CREATE TABLE taxish20150401_staggn215(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn215
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St215 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn220;
CREATE TABLE taxish20150401_STTPn220(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP220 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn220
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn220;
CREATE TABLE taxish20150401_staggn220(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn220
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St220 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn225;
CREATE TABLE taxish20150401_STTPn225(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP225 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn225
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn225;
CREATE TABLE taxish20150401_staggn225(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn225
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St225 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn230;
CREATE TABLE taxish20150401_STTPn230(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP230 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn230
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn230;
CREATE TABLE taxish20150401_staggn230(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn230
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St230 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
DROP TABLE IF EXISTS taxish20150401_STTPn235;
CREATE TABLE taxish20150401_STTPn235(i string,c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
FROM taxish20150401_STTP235 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPn235
SELECT distinct b.OBJECTID,a.c
WHERE b.BoundaryShape=a.area;DROP TABLE IF EXISTS taxish20150401_staggn235;
CREATE TABLE taxish20150401_staggn235(i string, c DOUBLE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile;
INSERT OVERWRITE TABLE taxish20150401_staggn235
SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp
JOIN taxish20150401_St235 ts
WHERE bp.objectid=ts.ctOBJECTID
GROUP BY bp.objectid;
