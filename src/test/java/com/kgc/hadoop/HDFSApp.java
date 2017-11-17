package com.kgc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

import static java.io.FileDescriptor.in;

/**
 * Created by kgc on 2017/11/17.
 */
public class HDFSApp {
    FileSystem fileSystem = null;
    Configuration configuration = null;
    public static final String HDFS_PATH="hdfs://192.168.85.128:8020";

    @Before
    public void setUp() throws Exception{
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
        System.out.println("setUp");
    }

    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        out.write("hello world\n".getBytes());
        out.flush();
        out.close();
    }


    @Test
    public void cat() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }

    @Test
    public void copyFromLocalFile() throws IOException {
        Path localPath = new Path("E:\\new.txt");
        Path hdfsPath = new Path("/hdfsapi/test/new.txt");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    @Test
    public void copyFromLocalFileWithProgress() throws  Exception{
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:\\BaiduNetdiskDownload\\hadoop-2.6.0-cdh5.7.0.tar.gz")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/hadoop-2.6.0-cdh5.7.0.tar.gz"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096);
    }

    @Test
    public void copyToLocalFile() throws IOException {
        Path localPath = new Path("E:\\fromHdfs.txt");
        Path hdfsPath = new Path("/hdfsapi/test/new.txt");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }

    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for(FileStatus file : fileStatuses){
            String isDir = file.isDirectory() ? "文件夹":"文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long size = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + size + "\t" + path);
        }
    }

    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi"), true);
    }
    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println("tearDown");
    }
}
