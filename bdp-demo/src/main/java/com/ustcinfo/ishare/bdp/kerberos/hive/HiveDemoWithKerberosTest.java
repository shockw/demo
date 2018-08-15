package com.ustcinfo.ishare.bdp.kerberos.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.security.UserGroupInformation;


public class HiveDemoWithKerberosTest {
	private static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	/** 连接hiveserver2 的jdbcURL，注意后面的principal是集群hive-site.xml配置文件中hive.server2.authentication.kerberos.principal的值 **/
	private static final String JDBC_URL = "jdbc:hive2://node182:10000/filtered;principal=hive/_HOST@HADOOP.COM";
	private static final String sql = "show tables";
	
	/** keytab文件 **/
	private static final String KEYTAB_FILE = "/Users/shock/Documents/shockspace/bdp-demo/src/main/resources/test.keytab";
	/** 主体名，与keytab文件中的主体保持一致 **/
	private static final String LOGIN_PRINCIPAL = "test/test";
	/** krb5.conf配置文件 **/
	private static final String KRB5_CONF = "/Users/shock/Documents/shockspace/bdp-demo/src/main/resources/krb5.conf";
	
	public static void main(String[] args) {
		try {
			System.setProperty("java.security.krb5.conf", KRB5_CONF);
            UserGroupInformation.loginUserFromKeytab(LOGIN_PRINCIPAL,KEYTAB_FILE);
            Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(JDBC_URL);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
