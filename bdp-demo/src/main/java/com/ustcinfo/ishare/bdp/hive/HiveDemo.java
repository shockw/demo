package com.ustcinfo.ishare.bdp.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.security.UserGroupInformation;


public class HiveDemo {
	private static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	/** 连接hiveserver2 的jdbcURL，注意后面的principal是集群hive-site.xml配置文件中hive.server2.authentication.kerberos.principal的值 **/
	private static final String JDBC_URL = "jdbc:hive2://node182:10000";
	private static final String sql = "show tables";
	
	public static void main(String[] args) {
		try {
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
