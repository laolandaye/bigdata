package com.atguigu.hdfs._2API;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestCopyFromLocalFile {

    // 1 文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取fs对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");   // 测试参数优先级
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop137:9000"), conf , "atguigu");

        // 2 执行上传API
        fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banzhang.txt"));

        // 3 关闭资源
        fs.close();
    }
}
