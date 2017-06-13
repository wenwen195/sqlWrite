import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * Created by ryw on 2017/5/14.
 */
//将输出的分析结果文件从单行记录为一个json转化为整个文件为一个Json
// 在Hadoop中编译运行，整个文件放入$HADOOP_CLASSPATH中
//javac -classpath ../share/hadoop/common/hadoop-common-2.5.2.jar JsonChange.java
//hadoop JsonChange
public class JsonChange {
    public static void toJson(String uriOld,String uri) throws Exception {

        long startTime=System.currentTimeMillis();
        InputStream in = null;
        OutputStream out = null;

        Configuration conf = new Configuration();
        FileSystem fsOld = FileSystem. get(URI.create (uriOld), conf);
        try {
            in = fsOld.open(new Path(uriOld));

            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            out = fs.create(new Path(uri), new Progressable() {

                @Override
                public void progress() {
                    System.out.print("");
                }

            });

            String sTmp="[\n";
            out.write(sTmp.getBytes());
            byte[] buffer = new byte[1];
            int bytesRead = in.read(buffer);
            while (bytesRead>0){
                if(new String(buffer).equals("\n")){
                    sTmp=",\n";
                    bytesRead = in.read(buffer);
                    if(bytesRead>0) {
                        out.write(sTmp.getBytes());
                    }
                }else {
                    out.write(buffer, 0, bytesRead);
                    bytesRead = in.read(buffer);
                }
            }
            sTmp="]";
            out.write(sTmp.getBytes());

        } finally {

            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
        long endTime=System.currentTimeMillis();
        System.out.println("Times： "+(endTime-startTime)+"ms");
    }
    public static void main(String[] args) throws Exception {

        int year = 2015;
        int month = 04;
        int day = 05;

        String taxiTPre = "taxish" + String.format("%02d", year) + String.format("%02d", month) + String.format("%02d", day) + "_";

        String taxiVTb = taxiTPre + "value";
        toJson("/user/hive/warehouse/"+taxiVTb+"/000000_0","/taxiSH/"+taxiVTb+".json");
          for (int hour = 0; hour < 24; hour++) {
                for (int minute = 0; minute < 6; minute += 5) {
                    String time = String.format("%02d", hour) + minute;
                    String stOdTb = taxiTPre + "stod" + time;
                    toJson("/user/hive/warehouse/"+stOdTb+"/000000_0","/taxiSH/"+stOdTb+".json");
//                    String stTpTb = taxiTPre + "sttp" + time;
//                    toJson("/user/hive/warehouse/"+stTpTb+"/000000_0","/taxiSH/"+stTpTb+".json");
//                    String stAggTb = taxiTPre + "stagg" + time;
//                    toJson("/user/hive/warehouse/"+stAggTb+"/000000_0","/taxiSH/"+stAggTb+".json");
                    String agg1Tb = taxiTPre + "agg1" + time;
                    toJson("/user/hive/warehouse/"+agg1Tb+"/000000_0","/taxiSH/"+agg1Tb+".json");
                    String agg2Tb = taxiTPre + "agg2" + time;
                    toJson("/user/hive/warehouse/"+agg2Tb+"/000000_0","/taxiSH/"+agg2Tb+".json");
                    String stTpTb = taxiTPre + "sttpn" + time;
                    toJson("/user/hive/warehouse/"+stTpTb+"/000000_0","/taxiSH/"+stTpTb+".json");
                    String stAggTb = taxiTPre + "staggn" + time;
                    toJson("/user/hive/warehouse/"+stAggTb+"/000000_0","/taxiSH/"+stAggTb+".json");
                }
            }

    }
}
