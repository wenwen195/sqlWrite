import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by ryw on 2017/6/1.
 */
//在windows中安装wget后用于生成批量下载结果文件的.sh文件，直接双击即可运行
public class WgetUse {
    public static void main(String[] args) {
        try {
            FileWriter writer = new FileWriter("Wget2.sh");
            BufferedWriter bw = new BufferedWriter(writer);

            for (int hour=0;hour<24;hour++){
                for (int minute=0;minute<6;minute+=5){
                    String time=String.format("%02d",hour)+minute;
                    String stTpTbP="taxish20150401_sttpp"+time;
                    String stAggTbP="taxish20150401_staggp"+time;
                    String stODTbP="taxish20150401_stodp"+time;
                    String stODTbF="taxish20150401_stodf"+time;
                    String wgetTp="wget --output-document=D:/17/GraduationDesign/gistoolshdp/final/"+stTpTbP+".csv  \"http://10.10.240.118:50075/webhdfs/v1/user/hive/warehouse/"+stTpTbP+"/000000_0?op=OPEN&namenoderpcaddress=10.10.240.118:9000&offset=0\"\n";
                    String wgetAp="wget --output-document=D:/17/GraduationDesign/gistoolshdp/final/"+stAggTbP+".csv \"http://10.10.240.118:50075/webhdfs/v1/user/hive/warehouse/"+stAggTbP+"/000000_0?op=OPEN&namenoderpcaddress=10.10.240.118:9000&offset=0\"\n";
                    String wgetOdp="wget --output-document=D:/17/GraduationDesign/gistoolshdp/final/"+stODTbP+".csv  \"http://10.10.240.118:50075/webhdfs/v1/user/hive/warehouse/"+stODTbP+"/000000_0?op=OPEN&namenoderpcaddress=10.10.240.118:9000&offset=0\"\n";
                    String wgetOdf="wget --output-document=D:/17/GraduationDesign/gistoolshdp/final/"+stODTbF+".csv \"http://10.10.240.118:50075/webhdfs/v1/user/hive/warehouse/"+stODTbF+"/000000_0?op=OPEN&namenoderpcaddress=10.10.240.118:9000&offset=0\"\n";
//                    bw.write(wgetAp);
//                    bw.write(wgetTp);
                    bw.write(wgetOdp);
                    bw.write(wgetOdf);
                }
            }

            bw.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
