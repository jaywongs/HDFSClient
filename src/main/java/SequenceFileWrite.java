import java.io.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class SequenceFileWrite {
    public static void main (String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/core-site.xml"));
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/hdfs-site.xml"));

        //得到输出流
        String seqFS = "/user/wsj/seq/sequenceFile";
        FileSystem fs = FileSystem.get(conf);
        Path seqPath = new Path(seqFS);
        SequenceFile.Writer writer = null;
        Text value = new Text();
        Text key = new Text();

        writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(seqPath),
                SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()));
        //得到输入流
        String filePath = "/Users/wangshijie/Pictures/Screenshots";
        File localPath = new File(filePath);
        String[] localFilesString = localPath.list();
        int fileNum = localFilesString.length;
        System.out.println(fileNum);
        //读取
        while (fileNum > 0) {
            File file = new File(filePath + "/" + localFilesString[fileNum - 1]);
            System.out.println(file.getPath());
            key = new Text(file.getName());
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            long len = file.length();
            byte[] buffer = new byte[(int) len];
            if ((len = in.read(buffer)) != -1) {
                value.set(buffer);
                writer.append(key, value);
            }

            //资源的回收
            value.clear();
            in.close();
            fileNum--;
        }
    }
}
