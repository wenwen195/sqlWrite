import java.io.*;

/**
 * Created by ryw on 2017/5/1.
 */
//调用oneDay的主函数，可定义输出某一天或者某几天，一天一个Sql文件
public class sqlWrite {

    public static void main(String[] args) {

        int year=2015;
        int month=04;
        int day=01;

        oneDay oD=new oneDay();
        //可以添加万年历的循环或者特定输出某一天
        oD.runOneDay(year,month,day);
    }
}





