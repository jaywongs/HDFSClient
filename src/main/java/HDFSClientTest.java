import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

public class HDFSClientTest {
    public static void main (String[] args) throws IOException {
        //set config
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://172.16.132.74:8020");
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/core-site.xml"));
        conf.addResource(new Path("/Users/wangshijie/Desktop/HDFSClient/src/main/resources/hdfs-site.xml"));
//
        FileSystem fs = FileSystem.get(conf);

//          list directory's file
//        FileStatus[] fsStatus = fs.listStatus(new Path("/user/wsj"));
//        for (int i = 0; i < fsStatus.length; i++) {
//            System.out.println(fsStatus[i].getPath().toString());
//        }

//        下载到本地
//        FSDataInputStream fsdi = fs.open(new Path("/user/wsj/test_5M.txt"));
//        OutputStream output = new FileOutputStream("/Users/wangshijie/Desktop/test_5M.txt");
//        IOUtils.copyBytes(fsdi,output,4096,true);

        //本地上传
//        Path src = new Path("/Users/wangshijie/Desktop/Transwarp/Srcdata/cq/201806102049340010711028.txt.gz");
//        Path des = new Path("/user/wsj/data/cq/201806102249340010711028.txt.gz");
//        fs.copyFromLocalFile(src, des);
//
//        FileUtil.copy(fs,src,fs,des,false,conf);

        //hdfs复制
//        Path src = new Path("/user/wsj/data/cq/201806102249340010711028.txt.gz");
//        Path des = new Path("/user/wsj/data/bj/201806101750340010711028.txt.gz");
//        FileUtil.copy(fs,src,fs,des,false,conf);

        //输出到控制台
//        FSDataInputStream is = fs.open(new Path("/user/wsj/password.txt"));
//        byte[] buff = new byte[1024];
//        int length = 0;
//        while ((length = is.read(buff)) != -1){
//            System.out.println(new String(buff,0,length));
//        }
//        System.out.println(fs.getClass().getName());
        //新建文件夹
//        Path path = new Path("/user/wsj/data/hb");
//        fs.mkdirs(path);

        //删除
        Path path = new Path("/user/wsj/test/cq");
        fs.delete(path,true);

    }
}
