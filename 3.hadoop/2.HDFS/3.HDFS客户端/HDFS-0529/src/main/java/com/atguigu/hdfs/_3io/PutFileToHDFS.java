package com.atguigu.hdfs._3io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class PutFileToHDFS {
    // 把本地e盘上的banhua.txt文件上传到HDFS根目录
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop137:9000"), conf , "atguigu");

        // 2 获取输入流
        FileInputStream fis = new FileInputStream(new File("e:/banzhang.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banzhang_new.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
