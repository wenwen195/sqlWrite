import java.io.*;

/**
 * Created by ryw on 2017/5/1.
 */
//生成删除hive中所有表的语句，输出文件DropAllTable.sql
//从hive中用show tables;读出所以数据库，粘贴到某文本中作为输入，输出文件为sql文件，在hive中通过nohup hive -f xx.sql;执行
public class dropAllTable {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            StringBuffer sb = new StringBuffer("");
            FileReader reader = new FileReader("D:\\17\\GraduationDesign\\gistoolshdp\\allTables.txt");
            BufferedReader br = new BufferedReader(reader);

            String str = null;

            while ((str = br.readLine()) != null) {
                sb.append("DROP TABLE IF EXISTS " + str + ";\n");

                System.out.println(str);
            }

            br.close();
            reader.close();

            // write string to file
            FileWriter writer = new FileWriter("DropAllTable.sql");
            BufferedWriter bw = new BufferedWriter(writer);
            bw.write(sb.toString());

            bw.close();
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
