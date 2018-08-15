package com.ustcinfo.ishare.bdp.kerberos.hdfs;

import java.io.IOException;
import java.net.URI;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;


public class HdfsDemoWithKerberosTest {
	private static FileSystem fs;
	private static Configuration conf;
	/** keytab文件 **/
	private static final String KEYTAB_FILE = "C:/kafka/hdfs/hdfs.keytab";
	/** 主体名，与keytab文件中的主体保持一致 **/
	private static final String LOGIN_PRINCIPAL = "hdfs@HADOOP.COM";
	/** krb5.conf配置文件 **/
	private static final String KRB5_CONF = "C:/kafka/krb5.conf";

	KerberosPrincipal p ;
	static {
		try {
			/** 设置krb5.conf配置文件路径 **/
			System.setProperty("java.security.krb5.conf", KRB5_CONF);
			conf = new Configuration();
			/** 初始化conf对象，加载hdfs-site.xml、core-site.xml等配置文件 **/
			UserGroupInformation.setConfiguration(conf);
			/** Kerberos登录认证 **/
			UserGroupInformation.loginUserFromKeytab(LOGIN_PRINCIPAL,
					KEYTAB_FILE);
			fs = FileSystem.get(URI.create(conf.get("fs.defaultFS")), conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		HdfsDemoWithKerberosTest hd = new HdfsDemoWithKerberosTest();
		hd.list("/test1");
		hd.close();
	}

	public void list(String folder) {
		System.out.println("ls: " + folder);
		if (!folder.endsWith("/")) {
			folder += "/";
		}
		System.out.println(
				"==========================================================");
		FileStatus[] list;
		try {
			list = fs.listStatus(new Path(folder));
			for (FileStatus f : list) {
				System.out.println(
						folder + f.getPath().getName() + "\t" + f.getOwner());
			}
			System.out.println(
					"==========================================================");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void close() {
		if (fs != null) {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
