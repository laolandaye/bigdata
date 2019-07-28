package com.atguigu.hdfs._2API;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestCopyToLocalFile {

    // 2 文件下载
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop137:9000"), conf , "atguigu");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
//        fs.copyToLocalFile(new Path("/banzhang.txt"), new Path("e:/banhua.txt"));
        fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/xiaohua.txt"), true);

        // 3 关闭资源
        fs.close();
    }
}
