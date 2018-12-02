package com.cody.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 通过hdfs协议，进行hdfs操作；
 * 需要注意HADOOP_USER_NAME需要设置为有访问权限的用户，
 * 否则报权限不足错误
 */
public class HdfsClient {

    private FileSystem fs = null;

    /**
     * 初始化
     * @throws IOException
     */
    @Before
    public void getFs() throws IOException {
        // 设置用户，远程访问的时候，必须设置有访问权限的用户名，否则报出权限不足错误
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration config = new Configuration();
        config.set("fs.defaultFS","hdfs://centos1:8020/");

        fs = FileSystem.get(config);
    }

    @After
    public void destory() throws IOException {
        fs.close();
    }

    /**
     * 从 hdfs 上传文件
     * @throws IOException
     */
    @Test
    public void upload() throws IOException {
        Path destFile = new Path("hdfs://centos1:8020/test.pdf");
        FSDataOutputStream output = fs.create(destFile);
//        fs.copyFromLocalFile();
        FileInputStream input = new FileInputStream("E:\\大数据\\hadoop02\\Hadoop技术内幕：深入解析YARN架构设计与实现原理.pdf");
        IOUtils.copy(input,output);
    }

    /**
     * 从 hdfs 下载文件到本地
     * @throws IOException
     */
    @Test
    public void download() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("hdfs://centos1:8020/test.pdf"));
        FileOutputStream outputStream = new FileOutputStream("test.pdf");
        IOUtils.copy(inputStream,outputStream);
//        fs.copyToLocalFile();
    }

    /**
     * 删除当前用户目录下的 a
     * @throws IOException
     */
    @Test
    public void rm() throws IOException {
        fs.delete(new Path("a"),true);
    }

    /**
     * 在当前用户下创建目录
     * @throws IOException
     */
    @Test
    public void mkDirs() throws IOException {
        fs.mkdirs(new Path("a/b/c"));
    }


}
