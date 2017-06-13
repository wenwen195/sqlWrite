import java.io.*;
import java.net.URI;
 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class CopyInHdfs {

    public static void main(String[] args) throws Exception {

        String uriOld = args[0];
        String uri = args[1]; 
        InputStream in = null;
        OutputStream out = null;
 
        Configuration conf = new Configuration();
        FileSystem fsOld = FileSystem. get(URI.create (uriOld), conf);
        BufferedReader reader = null;
    try {
        in = fsOld.open(new Path(uriOld));

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        out = fs.create(new Path(uri), new Progressable() {

                @Override
                public void progress() {
                    System.out.print("");
                }

            });

        String sTmp="{";
        out.write(sTmp.getBytes());
        byte[] buffer = new byte[1];
        int bytesRead = in.read(buffer);
        while (bytesRead>0){
            if(new String(buffer).equals("\n")){
                sTmp=",";
                out.write(sTmp.getBytes());
            }
            out.write(buffer, 0, bytesRead);
            bytesRead = in.read(buffer);
        }
        sTmp="}";
        out.write(sTmp.getBytes());

        } finally {

            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
