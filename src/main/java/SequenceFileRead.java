import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileRead {
    public static void main (String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/core-site.xml"));
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(conf);
        /**
         * 处理小文件上传
         *
         */
        Path path = new Path("/user/wsj/seq/sequenceFile");
        //1.无压缩
//        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
        //2.记录级压缩
//        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class, SequenceFile.CompressionType.RECORD);
        //3.块压缩
//        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class, SequenceFile.CompressionType.BLOCK);

//        File file = new File("/Users/wangshijie/Desktop/HDFSClient/Seq");
//        for (File f : file.listFiles()) {
//            writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
//        }
//        writer.close();

        /**
         * 查看sequencefile,循环获取小文件的内容
         *
         */
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
        Text value = new Text();
        //sequenceFile输出到文件
        Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);

        //循环写入
//        while(reader.next(key, value)){
//            //输出到控制台
//            String localPath = "/Users/wangshijie/Desktop/HDFSClient/Seq/OutputFile";
//            System.out.println(localPath + "/" + key);
//
//            //输出单个小文件
//            File file = new File(localPath + "/" +key.toString());
//            OutputStream out = new FileOutputStream(file);
//            out.write(value.getBytes(), 0, value.getLength());
//            value.clear();
//        }
        //输入文件名下载文件
        Scanner scanner = new Scanner(System.in);
        System.out.println("输入文件名：");
        String fileStr = scanner.next();
        System.out.println( "正在下载:" + fileStr + "\r");

        while(reader.next(key, value)){
            if (key.toString().equals(fileStr)){
                String localPath = "/Users/wangshijie/Desktop/HDFSClient/Seq/OutputFile";
                System.out.println(localPath + "/" + key);
                File file = new File(localPath + "/" +key.toString());
                OutputStream out = new FileOutputStream(file);
                out.write(value.getBytes(), 0, value.getLength());
                value.clear();
            }
        }

        //关闭资源
//        out.flush();
        reader.close();
//        out.close();

    }
}
