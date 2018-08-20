import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class TestTransToHDFS {

    public static void main (String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/core-site.xml"));
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/hdfs-site.xml"));
        //
        FileSystem fs = FileSystem.get(conf);
        File file = new File("/Users/wangshijie/Desktop/虚拟磁盘-s001.vmdk");
        FileInputStream fi = new FileInputStream(file);
        Path hdfsPath = new Path(URI.create("/user/wsj/data2/s001.vmdk"));
        FSDataOutputStream out = fs.create(hdfsPath);
        int bufSize = 4096;
        byte[] buf = new byte[bufSize];
        int read = fi.read(buf, 0, bufSize);
        while (-1 != read) {
            out.write(buf, 0, read);
            read = fi.read(buf, 0, bufSize);
        }
//        Thread.sleep(100000);
        fi.close();
        out.close();
    }
}
