package com.atguigu.hdfs._2API;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestListFiles {

    // 5 文件详情查看
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop137:9000"), conf , "atguigu");

        // 2 查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();

            // 查看文件名称、权限、长度、块信息
            System.out.println(fileStatus.getPath().getName());// 文件名称
            System.out.println(fileStatus.getPermission());// 文件权限
            System.out.println(fileStatus.getLen());// 文件长度

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("------班长分割线--------");
        }

        // 3 关闭资源
        fs.close();
    }
}
