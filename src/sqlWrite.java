import java.io.*;

/**
 * Created by ryw on 2017/5/1.
 */
public class sqlWrite {

    public static void main(String[] args) {
        String addjar="add jar\n" +
                "    /root/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar\n" +
                "    /root/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar;\n" +
                
                "create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';\n" +
                "create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';\n" +
                "create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';\n" +
                "create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';\n\n";
        String preBlock="CREATE EXTERNAL TABLE blocksh_v1p (Name string, objectid string, cx DOUBLE,cy DOUBLE,BoundaryShape binary)\n" +
                "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                "STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'\n" +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                
                "LOAD DATA INPATH  '/SHstreet/enCounty.json' OVERWRITE INTO TABLE blocksh_v1p;\n\n";
        int year=2015;
        int month=04;
        int day=01;
        String taxiTPre="taxish"+String.format("%02d",year)+String.format("%02d",month)+String.format("%02d",day)+"_";
        String taxiTb=taxiTPre;
        String taxiFl="part-"+month+"."+String.format("%02d",day)+".csv";
        String preTaxi="CREATE EXTERNAL TABLE "+taxiTb+"(carId DOUBLE,isAlarm DOUBLE,isEmpty DOUBLE,topLight DOUBLE,\n" +
                "Elevated DOUBLE,isBrake DOUBLE,receiveTime TIMESTAMP,GPSTime STRING,longitude DOUBLE,latitude DOUBLE,\n" +
                "speed DOUBLE,direction DOUBLE,satellite DOUBLE)\n" +
                "ROW FORMAT delimited fields terminated by ',' STORED AS textfile\n" +
                "tblproperties (\"skip.header.line.count\"=\"1\");\n" +
                
                "describe "+taxiTb+";\n" +
                
                "LOAD DATA INPATH '/taxidemo/"+taxiFl+"' OVERWRITE INTO TABLE "+taxiTb+";\n\n";
        String taxiVTb=taxiTPre+"value";
        String taxiV="CREATE TABLE "+taxiVTb+"(type STRING,time STRING,min DOUBLE,max DOUBLE)\n" +
                "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n";
        String taxiVPTb=taxiTPre+"valuep";
        String taxiVP="CREATE TABLE "+taxiVPTb+"(type STRING,time STRING,min DOUBLE,max DOUBLE);\n\n";

        try {
            FileWriter writer = new FileWriter("runOneDay.sql");
            BufferedWriter bw = new BufferedWriter(writer);

//            System.out.print(addjar);
            bw.write(addjar);
//            System.out.print(preBlock);
            bw.write(preBlock);
//            System.out.println(preTaxi);
            bw.write(preTaxi);
//            System.out.println(taxiV);
            bw.write(taxiV);
//            System.out.println(taxiVP);
            bw.write(taxiVP);
//            System.out.println();
//            bw.write();

            //24小时 每半小时一分
            for (int hour=0;hour<24;hour++){
                for (int minute=0;minute<6;minute+=5){
                    //原数据时间分割转存 Day divide****************************************************************************************************************************
                    String time=String.format("%02d",hour)+minute;
//                    System.out.println(time);
//                    bw.write(time);
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
                    String timeDiv="CREATE TABLE "+timeTb+"(carId DOUBLE,receiveTime TIMESTAMP,longitude DOUBLE,latitude DOUBLE);\n" +
                            
                            "describe "+timeTb+";\n" +
                            
                            "FROM (SELECT carId,receiveTime,longitude,latitude FROM "+taxiTb+" WHERE "+taxiTb+".receiveTime > '"+timeBg+"') taxish150401s\n" +
                            "INSERT OVERWRITE TABLE "+timeTb+
                            "SELECT *\n" +
                            "WHERE taxish150401s.receiveTime < '"+timeEn+"';\n\n";

//                    System.out.println(timeDiv);
                    bw.write(timeDiv);

                    //stOD*******************************************************************************************************************************************************************
                    String stTb=taxiTPre+"St"+time;
                    String stMaxTb=taxiTPre+"Stmax"+time;
                    String stMinTb=taxiTPre+"Stmin"+time;
                    String stOdPTb=taxiTPre+"STODp"+time;
                    String stOdFTb=taxiTPre+"STODf"+time;
                    String stOdTb=taxiTPre+"STOD"+time;
                    String stOD="CREATE EXTERNAL TABLE "+stTb+"(carId DOUBLE,receiveTime string,longitude DOUBLE,latitude DOUBLE,ctNAME string,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE);\n" +
                            
                            "INSERT OVERWRITE TABLE "+stTb+
                            "SELECT tt.carId,tt.receiveTime,tt.longitude, tt.latitude,bp.NAME, bp.OBJECTID, bp.cx, bp.cy\n" +
                            "FROM blocksh_v1p bp JOIN "+timeTb+" tt\n" +
                            "WHERE ST_Contains(bp.BoundaryShape, ST_Point(tt.longitude, tt.latitude));\n" +
                            
                            "drop table "+timeTb+";\n" +
                            
                            "CREATE TABLE "+stMaxTb+"(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,max int);\n" +
                            
                            "CREATE TABLE "+stMinTb+"(carId DOUBLE,ctOBJECTID int,ctcx DOUBLE,ctcy DOUBLE,min int);\n" +
                            
                            "FROM(SELECT t.carId carId,MAX(unix_timestamp(t.receiveTime)) max FROM "+stTb+" t GROUP BY t.carId) ts,"+stTb+" t\n" +
                            "INSERT OVERWRITE TABLE "+stMaxTb+
                            "SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.max\n" +
                            "WHERE t.carId=ts.carId and ts.max=unix_timestamp(t.receiveTime);\n" +
                            
                            "FROM(SELECT t.carId carId,MIN(unix_timestamp(t.receiveTime)) min FROM "+stTb+" t GROUP BY t.carId) ts,"+stTb+" t\n" +
                            "INSERT OVERWRITE TABLE "+stMinTb+
                            "SELECT distinct t.carId,t.ctOBJECTID,t.ctcx,t.ctcy,ts.min\n" +
                            "WHERE t.carId=ts.carId and ts.min=unix_timestamp(t.receiveTime);\n" +
                            
                            
                            "CREATE TABLE "+stOdPTb+"(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                            
                            "FROM(SELECT distinct tmin.carId carId,tmin.ctOBJECTID OctOBJECTID,tmin.ctcx Octcx,tmin.ctcy Octcy,tmax.ctOBJECTID DctOBJECTID,tmax.ctcx Dctcx,tmax.ctcy Dctcy FROM "+stMinTb+" tmin,"+stMaxTb+" tmax WHERE tmin.carId=tmax.carId) tod\n" +
                            "INSERT OVERWRITE TABLE "+stOdPTb+
                            "SELECT tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy, COUNT(*) count\n" +
                            "GROUP BY tod.OctOBJECTID,tod.Octcx,tod.Octcy,tod.DctOBJECTID,tod.Dctcx,tod.Dctcy;\n" +
                            
                            "CREATE TABLE "+stOdFTb+"(OctOBJECTID int,Octcx DOUBLE,Octcy DOUBLE,DctOBJECTID int,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE);\n" +
                            
                            
                            "INSERT OVERWRITE TABLE "+stOdFTb+
                            "SELECT od1.OctOBJECTID,od1.Octcx,od1.Octcy,od1.DctOBJECTID,od1.Dctcx,od1.Dctcy,od1.count-od2.count\n" +
                            "FROM "+stOdPTb+" od1 JOIN "+stOdPTb+" od2\n" +
                            "WHERE od1.OctOBJECTID=od2.DctOBJECTID and od1.DctOBJECTID=od2.OctOBJECTID;\n" +
                            
                            "CREATE TABLE "+stOdTb+"(Octcx DOUBLE,Octcy DOUBLE,Dctcx DOUBLE,Dctcy DOUBLE,count DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                            
                            "INSERT OVERWRITE TABLE "+stOdTb+
                            "SELECT Octcx,Octcy,Dctcx,Dctcy,count FROM "+stOdFTb+
                            "WHERE count>0;\n" +
                            
                            "drop table "+stMaxTb+";\n" +
                            "drop table "+stMinTb+";\n" +
                            "drop table "+stOdFTb+";\n";
                    String odVInsert="FROM "+stOdTb+
                            "INSERT INTO TABLE "+taxiVPTb+
                            "SELECT \"STOD\",\"185\",MIN(count),MAX(count);\n\n";

//                    System.out.println(stOD);
                    bw.write(stOD);
//                    System.out.println(odVInsert);
                    bw.write(odVInsert);

                    //stTp*************************************************************************************************************************************************************
                    String stOTb=taxiTPre+"STO"+time;
                    String stDTb=taxiTPre+"STD"+time;
                    String stTpTb=taxiTPre+"STTP"+time;
                    String stTp="CREATE TABLE "+stOTb+"(OctOBJECTID int,count DOUBLE);\n" +
                            
                            "CREATE TABLE "+stDTb+"(DctOBJECTID int,count DOUBLE);\n" +
                            
                            "INSERT OVERWRITE TABLE "+stOTb+
                            "SELECT OctOBJECTID,SUM(count)\n" +
                            "FROM "+stOdPTb+
                            "GROUP BY OctOBJECTID,Octcx,Octcy;\n" +
                            
                            "INSERT OVERWRITE TABLE "+stDTb+
                            "SELECT DctOBJECTID,SUM(count)\n" +
                            "FROM "+stOdPTb+
                            "GROUP BY DctOBJECTID,Dctcx,Dctcy;\n" +
                            
                            "CREATE TABLE "+stTpTb+"(area BINARY,tpcount DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                            
                            "FROM "+stOTb+" o,"+stDTb+" d, blocksh_v1p b\n" +
                            "INSERT OVERWRITE TABLE "+stTpTb+
                            "SELECT distinct b.BoundaryShape,o.count-d.count\n" +
                            "WHERE o.OctOBJECTID=d.DctOBJECTID and b.OBJECTID=o.OctOBJECTID;\n" +
                            
                            "drop table "+stOTb+";\n" +
                            "drop table "+stDTb+";\n" +
                            "drop table "+stOdPTb+";\n";
                    String stTpVInsert="FROM "+stTpTb+
                            "INSERT INTO TABLE "+taxiVPTb+
                            "SELECT \"STTP\",\""+time+"\",MIN(tpcount),MAX(tpcount);\n\n";

//                    System.out.println(stTp);
                    bw.write(stTp);
//                    System.out.println(stTpVInsert);
                    bw.write(stTpVInsert);

                    //stagg**************************************************************************************************************************************************
                    String stAggTb=taxiTPre+"stagg"+time;
                    String stAgg="CREATE TABLE "+stAggTb+"(area BINARY, stcount DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                            
                            "INSERT OVERWRITE TABLE "+stAggTb+
                            "SELECT bp.BoundaryShape, count(*) cnt FROM blocksh_v1p bp\n" +
                            "JOIN "+stTb+" ts\n" +
                            "WHERE bp.objectid=ts.ctOBJECTID\n" +
                            "GROUP BY bp.BoundaryShape;";
                    String stAggVInsert="FROM "+stAggTb+
                            "INSERT INTO TABLE "+taxiVPTb+
                            "SELECT \"STAGG\",\"\"+time+\"\",MIN(stcount),MAX(stcount);\n\n";

//                    System.out.println(stTp);
                    bw.write(stTp);
//                    System.out.println(stAggVInsert);
                    bw.write(stAggVInsert);

                    //agg***************************************************************************************************************************************************

                    String agg1Tb=taxiTPre+"agg1"+time;
                    String agg2Tb=taxiTPre+"agg2"+time;
                    String agg="CREATE TABLE "+agg1Tb+"(area BINARY, count DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                            
                            "FROM (SELECT ST_Bin(0.01, ST_Point(longitude,latitude)) bin_id, *FROM "+stTb+") bins\n" +
                            "INSERT OVERWRITE TABLE "+agg1Tb+
                            "SELECT ST_BinEnvelope(0.01, bin_id) shape, COUNT(*) count\n" +
                            "GROUP BY bin_id;\n" +
                            
                            "CREATE TABLE "+agg2Tb+"(area BINARY, count DOUBLE)\n" +
                            "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              \n" +
                            "STORED AS INPUTFORMAT 'com.esri.json.hadoop.UnenclosedJsonInputFormat'\n" +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';\n" +
                            
                            "FROM (SELECT ST_Bin(0.02, ST_Point(longitude,latitude)) bin_id, *FROM "+stTb+") bins\n" +
                            "INSERT OVERWRITE TABLE "+agg2Tb+
                            "SELECT ST_BinEnvelope(0.02, bin_id) shape, COUNT(*) count\n" +
                            "GROUP BY bin_id;\n" +
                            "\n";
                    String aggVInsert="FROM "+agg1Tb+
                            "INSERT INTO TABLE "+taxiVPTb+
                            "SELECT \"AGG1\",\"185\",MIN(count),MAX(count);\n" +
                            
                            "FROM "+agg2Tb+
                            "INSERT INTO TABLE "+taxiVPTb+
                            "SELECT \"AGG2\",\"185\",MIN(count),MAX(count);\n\n";
//                    System.out.println(agg);
                    bw.write(agg);
//                    System.out.println(aggVInsert);
                    bw.write(aggVInsert);

                }
            }

            //value all*********************************************************************************************************************************************
            String valueAllInsert="INSERT INTO TABLE "+taxiVTb+
                    "SELECT * FROM "+taxiVPTb+";\n" +
                    
                    "drop table "+taxiVPTb+";\n";
//            System.out.println(valueAllInsert);
            bw.write(valueAllInsert);

            bw.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}





