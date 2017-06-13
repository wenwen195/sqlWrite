add jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar
    /root/esri-git/json-serde-1.3.8-jar-with-dependencies.jar
    /root/esri-git/json-udf-1.3.8-jar-with-dependencies.jar;
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';

CREATE TABLE taxish20150401_valuep(p STRING,m STRING,n DOUBLE,x DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_value
INSERT OVERWRITE TABLE taxish20150401_valuep
SELECT * ;CREATE TABLE taxish20150401_STTPp000(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP000 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp000
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp000(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg000 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp000
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp005(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP005 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp005
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp005(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg005 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp005
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp010(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP010 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp010
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp010(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg010 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp010
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp015(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP015 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp015
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp015(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg015 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp015
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp020(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP020 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp020
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp020(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg020 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp020
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp025(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP025 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp025
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp025(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg025 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp025
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp030(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP030 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp030
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp030(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg030 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp030
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp035(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP035 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp035
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp035(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg035 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp035
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp040(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP040 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp040
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp040(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg040 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp040
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp045(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP045 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp045
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp045(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg045 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp045
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp050(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP050 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp050
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp050(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg050 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp050
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp055(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP055 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp055
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp055(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg055 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp055
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp060(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP060 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp060
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp060(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg060 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp060
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp065(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP065 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp065
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp065(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg065 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp065
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp070(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP070 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp070
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp070(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg070 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp070
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp075(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP075 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp075
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp075(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg075 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp075
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp080(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP080 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp080
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp080(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg080 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp080
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp085(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP085 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp085
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp085(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg085 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp085
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp090(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP090 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp090
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp090(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg090 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp090
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp095(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP095 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp095
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp095(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg095 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp095
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp100(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP100 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp100
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp100(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg100 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp100
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp105(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP105 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp105
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp105(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg105 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp105
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp110(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP110 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp110
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp110(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg110 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp110
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp115(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP115 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp115
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp115(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg115 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp115
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp120(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP120 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp120
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp120(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg120 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp120
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp125(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP125 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp125
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp125(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg125 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp125
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp130(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP130 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp130
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp130(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg130 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp130
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp135(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP135 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp135
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp135(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg135 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp135
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp140(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP140 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp140
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp140(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg140 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp140
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp145(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP145 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp145
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp145(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg145 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp145
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp150(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP150 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp150
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp150(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg150 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp150
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp155(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP155 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp155
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp155(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg155 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp155
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp160(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP160 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp160
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp160(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg160 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp160
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp165(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP165 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp165
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp165(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg165 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp165
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp170(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP170 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp170
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp170(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg170 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp170
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp175(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP175 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp175
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp175(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg175 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp175
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp180(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP180 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp180
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp180(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg180 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp180
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp185(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP185 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp185
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp185(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg185 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp185
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp190(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP190 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp190
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp190(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg190 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp190
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp195(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP195 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp195
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp195(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg195 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp195
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp200(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP200 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp200
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp200(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg200 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp200
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp205(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP205 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp205
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp205(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg205 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp205
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp210(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP210 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp210
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp210(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg210 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp210
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp215(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP215 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp215
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp215(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg215 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp215
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp220(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP220 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp220
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp220(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg220 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp220
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp225(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP225 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp225
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp225(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg225 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp225
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp230(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP230 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp230
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp230(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg230 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp230
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_STTPp235(name STRING,c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_STTP235 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_STTPp235
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

CREATE TABLE taxish20150401_staggp235(name STRING, c DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile;
FROM taxish20150401_stagg235 a, blocksh_v1p b
INSERT OVERWRITE TABLE taxish20150401_staggp235
SELECT b.Name,a.c
WHERE b.BoundaryShape=a.area;

