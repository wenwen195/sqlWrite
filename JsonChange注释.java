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
        long startTime=System.currentTimeMillis();//计时器，开始计时时间点
        InputStream in = null;//初始化输入输出数据流
        OutputStream out = null;
        Configuration conf = new Configuration();
        //获取不封闭分析结果Json文件
        FileSystem fsOld = FileSystem. get(URI.create (uriOld), conf);
        try {
            in = fsOld.open(new Path(uriOld));
            //输出文件创建
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            out = fs.create(new Path(uri), new Progressable() {
                @Override
                public void progress() {
                    System.out.print("");
                }
            });
            //在文件开始写入“[”
            String sTmp="[\n";
            out.write(sTmp.getBytes());
            byte[] buffer = new byte[1];
            int bytesRead = in.read(buffer);
            //对流数据按字符读取读取
            while (bytesRead>0){                
                //换行前写入行记录间的“,”分隔符
                if(new String(buffer).equals("\n")){                   
                    sTmp=",\n";
                    bytesRead = in.read(buffer);                    
                    if(bytesRead>0) {//分隔符需要不是文件结尾添加
                        out.write(sTmp.getBytes());
                    }
                }else {
                    //写入读取的行数据流
                    out.write(buffer, 0, bytesRead);
                    bytesRead = in.read(buffer);
                }
            }
            sTmp="]";//在文件结束写入“]”
            out.write(sTmp.getBytes());

        } finally {
            //关闭输入输出流
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
        long endTime=System.currentTimeMillis();
        System.out.println("Times： "+(endTime-startTime)+"ms");
    }
    public static void main(String[] args) throws Exception {
        //循环批量读取文件进行转换，有很多层，在后文分析中介绍
        for (; ; ) {
            String stTpTb = taxiTPre + "sttp" + time;
            //调用toJson(输入文件（分析结果），输出文件（Json))
            toJson("/user/hive/warehouse/"+stTpTb+"/000000_0","/taxiSH/"+stTpTb+".json");
            ......下方
        }
}

        int year = 2015;
        int month = 04;
        int day = 01;

        String taxiTPre = "taxish" + String.format("%02d", year) + String.format("%02d", month) + String.format("%02d", day) + "_";

        String taxiVTb = taxiTPre + "value";
        toJson("/user/hive/warehouse/"+taxiVTb+"/000000_0","/taxiSH/"+taxiVTb+".json");
          for (int hour = 0; hour < 24; hour++) {
                for (int minute = 0; minute < 6; minute += 5) {
                    String time = String.format("%02d", hour) + minute;
//                    String stOdTb = taxiTPre + "stod" + time;
//                    toJson("/user/hive/warehouse/"+stOdTb+"/000000_0","/taxiSH/"+stOdTb+".json");
//                    String stTpTb = taxiTPre + "sttp" + time;
//                    toJson("/user/hive/warehouse/"+stTpTb+"/000000_0","/taxiSH/"+stTpTb+".json");
//                    String stAggTb = taxiTPre + "stagg" + time;
//                    toJson("/user/hive/warehouse/"+stAggTb+"/000000_0","/taxiSH/"+stAggTb+".json");
//                    String agg1Tb = taxiTPre + "agg1" + time;
//                    toJson("/user/hive/warehouse/"+agg1Tb+"/000000_0","/taxiSH/"+agg1Tb+".json");
//                    String agg2Tb = taxiTPre + "agg2" + time;
//                    toJson("/user/hive/warehouse/"+agg2Tb+"/000000_0","/taxiSH/"+agg2Tb+".json");
                    String stTpTb = taxiTPre + "sttpn" + time;
                    toJson("/user/hive/warehouse/"+stTpTb+"/000000_0","/taxiSH/"+stTpTb+".json");
                    String stAggTb = taxiTPre + "staggn" + time;
                    toJson("/user/hive/warehouse/"+stAggTb+"/000000_0","/taxiSH/"+stAggTb+".json");
                }
            }

    }
}
