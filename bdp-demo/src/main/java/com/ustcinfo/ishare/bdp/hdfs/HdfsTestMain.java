package com.ustcinfo.ishare.bdp.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsTestMain {
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node56:9000");

		URI uri = new URI("/");
		FileSystem fs = FileSystem.get(conf);
		// RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path("/"),
		// false);
		// while(ri.hasNext()){
		// LocatedFileStatus status = ri.next();
		// String p = status.getPath().toString();
		// System.out.println(p);
		// }

		FileStatus[] fStatus = fs.listStatus(new Path("/"));
		for (int i = 0; i < fStatus.length; i++) {
			System.out.println(fStatus[i].toString());
		}
	}
}
