import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class HarFileSystemTest {
    public static void main (String[] args) throws IOException {

        Configuration conf = new Configuration();
        Path corePath = new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/core-site.xml");
        Path hdfsPath = new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/hdfs-site.xml");
        conf.addResource(corePath);
        conf.addResource(hdfsPath);

        HarFileSystem harF = new HarFileSystem();
        harF.initialize(URI.create("har:///user/wsj/HAR/harUnion.har"), conf);
        FileStatus[] listStatus = harF.listStatus(new Path("seqFile.txt"));
        for (FileStatus fileStatus: listStatus){
            System.out.println(fileStatus.getPath().toString());
        }

        //读取文件内容输出到控制台
        FSDataInputStream is = harF.open(new Path("seqFile.txt"));
        byte[] buff = new byte[1024];
        int length = 0;
//        while ((length = is.read(buff)) != -1){
//            System.out.println(new String(buff, 0, length));
//        }

        //保存到本地
        String localPath = "/Users/wangshijie/Desktop/HDFSClient/HAR/OutputFile";
        File file = new File(localPath + "/seqFile.txt");
        OutputStream out = new FileOutputStream(file);
        while ((length = is.read(buff)) != -1) {
            out.write(buff, 0, length);
        }
    }

}
