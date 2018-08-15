package com.ustcinfo.ishare.bdp.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDAO {
	private static String host = "node56";
	private static final String HDFS = "hdfs://" + host + ":9000";

	public HdfsDAO(Configuration conf) {
		this(HDFS, conf);
	}

	public HdfsDAO(String hdfs, Configuration conf) {
		this.hdfsPath = hdfs;
		this.conf = conf;
	}

	private String hdfsPath;
	private Configuration conf;

	public static void main(String[] args) throws IOException {
		Configuration conf = config();
		HdfsDAO hdfs = new HdfsDAO(conf);
		// hdfs.copyFile("datafile/item.csv", "/tmp/new");
		hdfs.ls("/");
	}

	public static Configuration config() {
		Configuration config = new Configuration();
		// 设置hdfs的通讯地址
		config.set("fs.defaultFS", HDFS);
		// 设置RN的主机
		config.set("yarn.resourcemanager.hostname", host);
		config.addResource("classpath:/hadoop/core-site.xml");
		return config;
	}

	public void mkdirs(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
			System.out.println("Create: " + folder);
		}
		fs.close();
	}

	public void rmr(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.deleteOnExit(path);
		System.out.println("Delete: " + folder);
		fs.close();
	}

	public void ls(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: " + folder);
		System.out.println("==========================================================");
		for (FileStatus f : list) {
			System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
		}
		System.out.println("==========================================================");
		fs.close();
	}

	public void createFile(String file, String content) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		byte[] buff = content.getBytes();
		FSDataOutputStream os = null;
		try {
			os = fs.create(new Path(file));
			os.write(buff, 0, buff.length);
			System.out.println("Create: " + file);
		} finally {
			if (os != null)
				os.close();
		}
		fs.close();
	}

	public void copyFile(String local, String remote) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from: " + local + " to " + remote);
		fs.close();
	}

	public void download(String remote, String local) throws IOException {
		Path path = new Path(remote);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyToLocalFile(path, new Path(local));
		System.out.println("download: from" + remote + " to " + local);
		fs.close();
	}

	public void cat(String remoteFile) throws IOException {
		Path path = new Path(remoteFile);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FSDataInputStream fsdis = null;
		System.out.println("cat: " + remoteFile);
		try {
			fsdis = fs.open(path);
			IOUtils.copyBytes(fsdis, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(fsdis);
			fs.close();
		}
	}

}
