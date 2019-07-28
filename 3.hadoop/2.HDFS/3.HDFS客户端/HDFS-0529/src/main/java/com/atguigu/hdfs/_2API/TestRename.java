package com.atguigu.hdfs._2API;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestRename {

    // 4 文件更名
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop137:9000"), conf , "atguigu");

        // 2 执行更名操作
        fs.rename(new Path("/banzhang.txt"), new Path("/yanjing.txt"));

        // 3 关闭资源
        fs.close();
    }
}
