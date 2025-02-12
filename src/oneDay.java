import java.io.*;

/**
 * Created by ryw on 2017/5/3.
 */
//循环半小时的HiveSql输出一天的出租车四种分析sql语句，输出文件runxxxx(日期).sql。HiveSQL注释见run-Oneday注释.sql
//sttpn和staggn是改进后输出街区ID不输出边界点的数据库，看需要取用，但sttpn需要sttp的结果，不能直接注释sttp，需要加一句drop如果不要原街区结果
public class oneDay {
    public void runOneDay(int year,int month,int day){
        String addjar="add jar\n" +
                "    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar\n" +
                "    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar\n" +
                "    /root/esri-git/json-serde-1.3.8-jar-with-dependencies.jar\n" +
                "    /root/esri-git/json-udf-1.3.8-jar-with-dependencies.jar;\n" +
                "create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';\n" +
                "create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';\n" +
                "create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';\n" +
                "create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';\n\n";
        String preBlock="DROP TABLE IF EXISTS blocksh_v1p;\n" +
                "CREATE EXTERNAL TABLE blocksh_v1p (Name string, objectid string, cx DOUBLE,cy DOUBLE,BoundaryShape binary)\n" +
                "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                "STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'\n" +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                "LOAD DATA INPATH  '/SHstreet/newEnCounty.json' OVERWRITE INTO TABLE blocksh_v1p;\n\n";

        String taxiTPre="taxish"+String.format("%02d",year)+String.format("%02d",month)+String.format("%02d",day)+"_";
        String taxiTb=taxiTPre;
        String taxiFl="part-"+month+"."+String.format("%02d",day)+".csv";
        String preTaxi="DROP TABLE IF EXISTS " +taxiTb+";\n"+
                "CREATE EXTERNAL TABLE "+taxiTb+"(carId DOUBLE,isAlarm DOUBLE,isEmpty DOUBLE,topLight DOUBLE,\n" +
                "Elevated DOUBLE,isBrake DOUBLE,receiveTime TIMESTAMP,GPSTime STRING,longitude DOUBLE,latitude DOUBLE,\n" +
                "speed DOUBLE,direction DOUBLE,satellite DOUBLE)\n" +
                "ROW FORMAT delimited fields terminated by ',' STORED AS textfile\n" +
                "tblproperties (\"skip.header.line.count\"=\"1\");\n" +

                "describe "+taxiTb+";\n" +

                "LOAD DATA INPATH '/taxidemo/"+taxiFl+"' OVERWRITE INTO TABLE "+taxiTb+";\n\n";
        String taxiVTb=taxiTPre+"value";
        String taxiV="DROP TABLE IF EXISTS " +taxiVTb+";\n"+
                "CREATE EXTERNAL TABLE "+taxiVTb+"(p STRING,m STRING,n DOUBLE,x DOUBLE)\n" +
                "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n" +
                "STORED AS textfile;\n";
        String taxiVPTb=taxiTPre+"valuepp";
        String taxiVP="DROP TABLE IF EXISTS " +taxiVPTb+";\n"+
                "CREATE TABLE "+taxiVPTb+"(time STRING,c DOUBLE);\n\n";
//        type STRING,time STRING,min DOUBLE,max DOUBLE
        String taxiVFTb=taxiTPre+"valuep";
        String taxiVp="CREATE TABLE "+taxiVFTb+"(p STRING,m STRING,n DOUBLE,x DOUBLE)\n" +
                "ROW FORMAT delimited fields terminated by ',' STORED AS textfile;\n" +
                "FROM "+taxiVTb+"\n" +
                "INSERT OVERWRITE TABLE "+taxiVFTb+"\n" +
                "SELECT * ;";
        try {
            FileWriter writer = new FileWriter("run"+String.format("%02d",year)+String.format("%02d",month)+String.format("%02d",day)+".sql");
            BufferedWriter bw = new BufferedWriter(writer);

//            System.out.print(addjar);
            bw.write(addjar);
//            System.out.print(preBlock);
//            bw.write(preBlock);
//            System.out.println(preTaxi);
            bw.write(preTaxi);
//            System.out.println(taxiV);
            bw.write(taxiV);
//            System.out.println(taxiVP);
            bw.write(taxiVP);
//            System.out.println();
//            bw.write();
//            bw.write(taxiVp);

            //24小时 每半小时一分
            for (int hour=0;hour<24;hour++){
                for (int minute=0;minute<6;minute+=5){
                    //原数据时间分割转存 Day divide****************************************************************************************************************************
                    String time=String.format("%02d",hour)+minute;
//                    System.out.println(time);
                    String timeTb=taxiTPre+"time"+time;
                    String timeBg=new String();
                    String timeEn=new String();
                    if(minute==0){
                        timeBg=year+"-"+String.format("%02d",month)+"-"+String.format("%02d",day)+" "+String.format("%02d",hour)+":00:00";
                        timeEn=year+"-"+String.format("%02d",month)+"-"+String.format("%02d",day)+" "+String.format("%02d",hour)+":30:00";
                    }else if (minute==5){
                        timeBg=year+"-"+String.format("%02d",month)+"-"+String.format("%02d",day)+" "+String.format("%02d",hour)+":30:00";
                        timeEn=year+"-"+String.format("%02d",month)+"-"+String.format("%02d",day)+" "+String.format("%02d",hour+1)+":00:00";
                    }
                    String timeDiv="DROP TABLE IF EXISTS " +timeTb+";\n"+
                            "CREATE TABLE "+timeTb+"(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);\n" +
                            "FROM (SELECT carId,receiveTime,longitude,latitude FROM "+taxiTb+" WHERE "+taxiTb+".receiveTime > '"+timeBg+"') taxishs\n" +
                            "INSERT OVERWRITE TABLE "+timeTb+
                            "\nSELECT *\n" +
                            "WHERE taxishs.receiveTime < '"+timeEn+"';\n\n";

//                    System.out.println(timeDiv);
                    bw.write(timeDiv);

                    //stOD*******************************************************************************************************************************************************************
                    String stTb=taxiTPre+"St"+time;
                    String stMaxTb=taxiTPre+"Stmax"+time;
                    String stMinTb=taxiTPre+"Stmin"+time;
                    String stOdPTb=taxiTPre+"STODp"+time;
                    String stOdFTb=taxiTPre+"STODf"+time;
                    String stOdTb=taxiTPre+"STOD"+time;
                    String stOD="DROP TABLE IF EXISTS " +stTb+";\n"+
                            "CREATE EXTERNAL TABLE "+stTb+"(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);\n" +

                            "INSERT OVERWRITE TABLE "+stTb+
                            "\nSELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy\n" +
                            "FROM blocksh_v1p bp JOIN "+timeTb+" tt\n" +
                            "WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));\n" +

                            "drop table "+timeTb+";\n" +

                            "DROP TABLE IF EXISTS " +stMaxTb+";\n"+
                            "CREATE TABLE "+stMaxTb+"(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);\n" +
                            "DROP TABLE IF EXISTS " +stMinTb+";\n"+
                            "CREATE TABLE "+stMinTb+"(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);\n" +

                            "FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM "+stTb+" t GROUP BY t.carId) ts,"+stTb+" t\n" +
                            "INSERT OVERWRITE TABLE "+stMaxTb+
                            "\nSELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max\n" +
                            "WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);\n" +

                            "FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM "+stTb+" t GROUP BY t.carId) ts,"+stTb+" t\n" +
                            "INSERT OVERWRITE TABLE "+stMinTb+
                            "\nSELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min\n" +
                            "WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);\n" +

                            "DROP TABLE IF EXISTS " +stOdPTb+";\n"+
                            "CREATE TABLE "+stOdPTb+"(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);\n" +

                            "FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM "+stMinTb+" tmin,"+stMaxTb+" tmax WHERE tmin.carId=tmax.carId) tod\n" +
                            "INSERT OVERWRITE TABLE "+stOdPTb+
                            "\nSELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count\n" +
                            "GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;\n" +

                            "DROP TABLE IF EXISTS " +stOdFTb+";\n"+
                            "CREATE TABLE "+stOdFTb+"(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);\n" +


                            "INSERT OVERWRITE TABLE "+stOdFTb+
                            "\nSELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count\n" +
                            "FROM "+stOdPTb+" od1 JOIN "+stOdPTb+" od2\n" +
                            "WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;\n" +

                            "DROP TABLE IF EXISTS " +stOdTb+";\n"+
                            "CREATE TABLE "+stOdTb+"(x DOUBLE,y DOUBLE,i DOUBLE,j DOUBLE,c DOUBLE)\n" +
                            "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n" +
                            "STORED AS textfile;\n" +

                            "INSERT OVERWRITE TABLE "+stOdTb+
                            "\nSELECT Octcx,Octcy,Dctcx,Dctcy,count FROM "+stOdFTb+
                            "\nWHERE count>0;\n" +

                            "drop table "+stMaxTb+";\n" +
                            "drop table "+stMinTb+";\n" +
                            "drop table "+stOdFTb+";\n";
                    String odVInsert="FROM "+stOdTb+
                            "\nINSERT INTO TABLE "+taxiVPTb+
                            "\nSELECT \"STOD\",\""+time+"\",MIN(c),MAX(c);\n\n";
                    String stVInsert="FROM "+stOdTb+
                            "\nINSERT INTO TABLE "+taxiVPTb+
                            "\nSELECT \"STOD\",\""+time+"\",MIN(c),MAX(c);\n\n";

//                    System.out.println(stOD);
                    bw.write(stOD);
//                    System.out.println(odVInsert);
                    bw.write(odVInsert);

                    //stTp*************************************************************************************************************************************************************
                    String stOTb=taxiTPre+"STO"+time;
                    String stDTb=taxiTPre+"STD"+time;
                    String stTpTb=taxiTPre+"STTP"+time;
                    String stTpTbN=taxiTPre+"STTPn"+time;
                    String stTp="DROP TABLE IF EXISTS " +stOTb+";\n"+
                            "CREATE TABLE "+stOTb+"(OctOBJECTID int,count DOUBLE);\n" +
                            "DROP TABLE IF EXISTS " +stDTb+";\n"+
                            "CREATE TABLE "+stDTb+"(DctOBJECTID int,count DOUBLE);\n" +

                            "INSERT OVERWRITE TABLE "+stOTb+
                            "\nSELECT OctOBJECTID,SUM(count)\n" +
                            "FROM "+stOdPTb+
                            "\nGROUP BY OctOBJECTID,Octcx,Octcy;\n" +

                            "INSERT OVERWRITE TABLE "+stDTb+
                            "\nSELECT DctOBJECTID,SUM(count)\n" +
                            "FROM "+stOdPTb+
                            "\nGROUP BY DctOBJECTID,Dctcx,Dctcy;\n" +

                            "DROP TABLE IF EXISTS " +stTpTb+";\n"+
                            "CREATE TABLE "+stTpTb+"(area BINARY,c DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +

                            "FROM "+stOTb+" o,"+stDTb+" d, blocksh_v1p b\n" +
                            "INSERT OVERWRITE TABLE "+stTpTb+
                            "\nSELECT distinct b.BoundaryShape,o.count-d.count\n" +
                            "WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;\n" +

                            "drop table "+stOTb+";\n" +
                            "drop table "+stDTb+";\n" +
                            "drop table "+stOdPTb+";\n";
                    String stTpN="DROP TABLE IF EXISTS "+stTpTbN+";\n" +
                            "CREATE TABLE "+stTpTbN+"(i string,c DOUBLE)\n" +
                            "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n" +
                            "STORED AS textfile;\n" +
                            "FROM "+stTpTb+" a, blocksh_v1p b\n" +
                            "INSERT OVERWRITE TABLE "+stTpTbN+"\n" +
                            "SELECT distinct b.OBJECTID,a.c\n" +
                            "WHERE b.BoundaryShape=a.area;";
                    String stTpVInsert="FROM "+stTb+
                            "\nINSERT INTO TABLE "+taxiVPTb+
                            "\nSELECT \""+time+"\",COUNT(*);\n\n";

//                    System.out.println(stTp);
                    bw.write(stTp);
//                    System.out.println(stTpVInsert);
//                    bw.write(stTpVInsert);
                    bw.write(stTpN);
//                    bw.write(stTpVInsert);

                    //stagg**************************************************************************************************************************************************
                    String stAggTb=taxiTPre+"stagg"+time;
                    String stAggTbN=taxiTPre+"staggn"+time;
                    String stAgg="DROP TABLE IF EXISTS " +stAggTb+";\n"+
                            "CREATE TABLE "+stAggTb+"(area BINARY, c DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +

                            "INSERT OVERWRITE TABLE "+stAggTb+
                            "\nSELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp\n" +
                            "JOIN "+stTb+" ts\n" +
                            "WHERE bp.objectid=ts.ctOBJECTID\n" +
                            "GROUP BY bp.BoundaryShape;\n";
                    String stAggN="DROP TABLE IF EXISTS " +stAggTbN+";\n" +
                            "CREATE TABLE " +stAggTbN+"(i string, c DOUBLE)\n" +
                            "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n" +
                            "STORED AS textfile;\n" +
                            "INSERT OVERWRITE TABLE "+stAggTbN+"\n" +
                            "SELECT bp.objectid, count(*) cnt FROM blocksh_v1p bp\n" +
                            "JOIN "+ stTb+" ts\n" +
                            "WHERE bp.objectid=ts.ctOBJECTID\n" +
                            "GROUP BY bp.objectid;\n";
                    String stAggVInsert="FROM "+stAggTb+
                            "\nINSERT INTO TABLE "+taxiVPTb+
                            "\nSELECT \"STAGG\",\""+time+"\",MIN(c),MAX(c);\n\n";

//                    System.out.println(stTp);
                    bw.write(stAgg);
//                    System.out.println(stAggVInsert);
//                    bw.write(stAggVInsert);
                    bw.write(stAggN);

                    //agg***************************************************************************************************************************************************

                    String agg1Tb=taxiTPre+"agg1"+time;
                    String agg2Tb=taxiTPre+"agg2"+time;
                    String agg="DROP TABLE IF EXISTS " +agg1Tb+";\n"+
                            "CREATE TABLE "+agg1Tb+"(area BINARY, c DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +

                            "FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM "+stTb+") bins\n" +
                            "INSERT OVERWRITE TABLE "+agg1Tb+
                            "\nSELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count\n" +
                            "GROUP BY bin_id;\n" +

                            "DROP TABLE IF EXISTS " +agg2Tb+";\n"+
                            "CREATE TABLE "+agg2Tb+"(area BINARY, c DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +

                            "FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM "+stTb+") bins\n" +
                            "INSERT OVERWRITE TABLE "+agg2Tb+
                            "\nSELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count\n" +
                            "GROUP BY bin_id;\n" +
                            "\n";
                    String aggVInsert="FROM "+agg1Tb+
                            "\nINSERT INTO TABLE "+taxiVPTb+
                            "\nSELECT \"AGG1\",\""+time+"\",MIN(c),MAX(c);\n" +

                            "FROM "+agg2Tb+
                            "\nINSERT INTO TABLE "+taxiVPTb+
                            "\nSELECT \"AGG2\",\""+time+"\",MIN(c),MAX(c);\n\n";
//                    System.out.println(agg);
                    bw.write(agg);
//                    System.out.println(aggVInsert);
                    bw.write(aggVInsert);
                    //for show*******************************************************************************************************************************************************
                    String stTpTbP=taxiTPre+"STTPp"+time;
                    String stAggTbP=taxiTPre+"staggp"+time;
                    String forPaper="CREATE TABLE "+stTpTbP+"(name STRING,c DOUBLE)\n" +
                            "ROW FORMAT delimited fields terminated by ',' STORED AS textfile;\n" +
                            "FROM "+stTpTb+" a, blocksh_v1p b\n" +
                            "INSERT OVERWRITE TABLE "+stTpTbP+"\n" +
                            "SELECT b.Name,a.c\n" +
                            "WHERE b.BoundaryShape=a.area;\n" +
                            "\n" +
                            "CREATE TABLE "+stAggTbP+"(name STRING, c DOUBLE)\n" +
                            "ROW FORMAT delimited fields terminated by ',' STORED AS textfile;\n" +
                            "FROM "+stAggTb+" a, blocksh_v1p b\n" +
                            "INSERT OVERWRITE TABLE "+stAggTbP+"\n" +
                            "SELECT b.Name,a.c\n" +
                            "WHERE b.BoundaryShape=a.area;\n\n";
                    String stODP="DROP TABLE IF EXISTS " +stMaxTb+";\n"+
                            "CREATE TABLE "+stMaxTb+"(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int)\n" +
                            "ROW FORMAT delimited fields terminated by ',' STORED AS textfile;\n" +
                            "DROP TABLE IF EXISTS " +stMinTb+";\n"+
                            "CREATE TABLE "+stMinTb+"(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int)\n" +
                            "ROW FORMAT delimited fields terminated by ',' STORED AS textfile;\n" +

                            "FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM "+stTb+" t GROUP BY t.carId) ts,"+stTb+" t\n" +
                            "INSERT OVERWRITE TABLE "+stMaxTb+
                            "\nSELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max\n" +
                            "WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);\n" +

                            "FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM "+stTb+" t GROUP BY t.carId) ts,"+stTb+" t\n" +
                            "INSERT OVERWRITE TABLE "+stMinTb+
                            "\nSELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min\n" +
                            "WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);\n" +

                            "DROP TABLE IF EXISTS " +stOdPTb+";\n"+
                            "CREATE TABLE "+stOdPTb+"(OctOBJECTID int,DctOBJECTID int,count DOUBLE)\n" +
                            "ROW FORMAT delimited fields terminated by ',' STORED AS textfile;\n" +

                            "FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmax.ctOBJECTID DctOBJECTID FROM "+stMinTb+" tmin,"+stMaxTb+" tmax WHERE tmin.carId=tmax.carId) tod\n" +
                            "INSERT OVERWRITE TABLE "+stOdPTb+
                            "\nSELECT tod.OctOBJECTID,tod.DctOBJECTID,COUNT(*) count\n" +
                            "GROUP BY tod.OctOBJECTID,tod.DctOBJECTID;\n" +

                            "DROP TABLE IF EXISTS " +stOdFTb+";\n"+
                            "CREATE TABLE "+stOdFTb+"(OctOBJECTID int,DctOBJECTID int,count DOUBLE)\n" +
                            "ROW FORMAT delimited fields terminated by ',' STORED AS textfile;\n" +

                            "INSERT OVERWRITE TABLE "+stOdFTb+
                            "\nSELECT od1.OctOBJECTID,od1.DctOBJECTID,od1.od1.count-od2.count\n" +
                            "FROM "+stOdPTb+" od1 JOIN "+stOdPTb+" od2\n" +
                            "WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;\n";
//                    bw.write(stODP);
                            ;

//                   bw.write(forPaper);
                }
            }

            //value all*********************************************************************************************************************************************
            String valueAllInsert="INSERT OVERWRITE TABLE "+taxiVTb+
                    " \nSELECT * FROM "+taxiVPTb+";\n" +

                    "drop table "+taxiVPTb+";\n";
            String valuePP="INSERT OVERWRITE TABLE "+taxiVPTb+
                    " \nSELECT * FROM "+taxiVPTb+";\n";
//            System.out.println(valueAllInsert);
            bw.write(valueAllInsert);
//            bw.write(valuePP);

            bw.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
