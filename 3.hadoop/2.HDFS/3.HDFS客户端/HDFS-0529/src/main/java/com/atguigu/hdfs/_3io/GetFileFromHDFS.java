package com.atguigu.hdfs._3io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class GetFileFromHDFS {

    // 从HDFS上下载banhua.txt文件到本地e盘上
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop137:9000"), conf , "atguigu");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/banzhang_new.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/banhua_old.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
