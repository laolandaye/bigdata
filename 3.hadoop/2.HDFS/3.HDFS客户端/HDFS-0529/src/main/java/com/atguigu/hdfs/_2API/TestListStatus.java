package com.atguigu.hdfs._2API;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestListStatus {
    // 6 判断是文件还是文件夹
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop137:9000"), conf , "atguigu");

        // 2 判断操作
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            if (fileStatus.isFile()) {
                // 文件
                System.out.println("文件:"+fileStatus.getPath().getName());
            }else{
                // 文件夹
                System.out.println("文件夹:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }
}
