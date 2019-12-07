package com.github.fancyideas.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.net.URI;

public class BzipUploadFileToHDFS {


    public static void main(String[] args) throws ClassNotFoundException, IOException {
        // 用Bzip压缩
        String codecClassName = "org.apache.hadoop.io.compress.BZip2Codec";
        Class<?> codecClass = Class.forName(codecClassName);
        // HDFS读写的配置文件
        HdfsConfiguration configuration = new HdfsConfiguration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
        // 数据源
        String originDataPath = "D:\\testdata\\bziptest.mp4";
        String destinationDataPath = "hdfs://node01:8020/bziptest.mp4.bz2";
        InputStream inputStream = new BufferedInputStream(new FileInputStream(originDataPath));
        FileSystem fileSystem = FileSystem.get(URI.create(destinationDataPath), configuration);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(destinationDataPath));
        //对输出流的数据压缩
        CompressionOutputStream compressedOut = codec.createOutputStream(fsDataOutputStream);
        IOUtils.copyBytes(inputStream, compressedOut, 4096);
    }
}
