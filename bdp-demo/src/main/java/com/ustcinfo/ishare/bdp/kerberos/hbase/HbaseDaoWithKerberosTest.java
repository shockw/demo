package com.ustcinfo.ishare.bdp.kerberos.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HbaseDaoWithKerberosTest {
	private static final Logger logger = LoggerFactory.getLogger(HbaseDaoWithKerberosTest.class);
	private static final String CHARACTER_SEPARATOR = ":";
	/** 集群配置对象 **/
	private static Configuration conf;
	/** 集群连接对象 **/
	private static Connection conn;
	/** hbase集群管理对象 **/
	private static Admin admin;

	/** keytab文件 **/
	private static final String KEYTAB_FILE = "/Users/shock/Documents/shockspace/bdp-demo/src/main/resources/zwang.keytab";
	/** 主体名，与keytab文件中的主体保持一致 **/
	private static final String LOGIN_PRINCIPAL = "zwang@HADOOP.COM";
	/** krb5.conf配置文件 **/
	private static final String KRB5_CONF = "/Users/shock/Documents/shockspace/bdp-demo/src/main/resources/krb5.conf";

	static {
		try {
			System.setProperty("java.security.krb5.conf", KRB5_CONF);
			System.out.println(KRB5_CONF);
			System.out.println(KEYTAB_FILE);
			conf = HBaseConfiguration.create();
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(LOGIN_PRINCIPAL,KEYTAB_FILE);
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
			closeAll();
		}
	}
	
	public static void main(String[] args) throws Exception {
		HbaseDaoWithKerberosTest hd = new HbaseDaoWithKerberosTest();
//		TableName tn = TableName.valueOf("test");
		hd.listTables();
		hd.scan("zwang_foo");
//		Put put = new Put("r2".getBytes());
//		put.addColumn("info".getBytes(), "info:age".getBytes(), "12".getBytes());
//		hd.put("test", put);
//		Table table = conn.getTable(tn);
//		Scan scan = new Scan();
//		ResultScanner rs = table.getScanner(scan);
//		Iterator<Result> it = rs.iterator();
//		while (it.hasNext()) {
//			Result result = it.next();
//			List<Cell> cells = result.listCells();
//			for (Cell cell : cells) {
//				String columnFamily = new String(CellUtil.cloneFamily(cell));
//				String qualifier = new String(CellUtil.cloneQualifier(cell));
//				String value = new String(CellUtil.cloneValue(cell));
//				System.out.println(columnFamily + CHARACTER_SEPARATOR
//						+ qualifier + CHARACTER_SEPARATOR + value);
//			}
//		}
		System.out.println("scan test table 结束");
//		hd.delete("test", "r2", "info", "info:age");
	}

	/**
	 * 删除表
	 * 
	 * @param tableName
	 *            表名
	 */
	public void deleteTable(String tableName) {
		TableName table = TableName.valueOf(tableName);
		try {
			if (admin.tableExists(table)) {
				admin.disableTable(table);
				admin.deleteTable(table);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 使表生效或失效
	 * 
	 * @param tableName
	 *            表名
	 * @param flag:
	 *            为true表示生效,false表示失效
	 */
	public void enOrDisableTable(String tableName, boolean flag) {
		TableName table = TableName.valueOf(tableName);
		try {
			if (admin.tableExists(table)) {
				if (flag) {
					admin.enableTable(table);
				} else {
					admin.disableTable(table);
				}
			}
		} catch (IOException e) {
			logger.info("表不存在tableName={}", tableName);
			e.printStackTrace();
		}
	}

	/**
	 * 清空表
	 * 
	 * @param tableName
	 *            表名
	 */
	public void truncTable(String tableName) {
		TableName table = TableName.valueOf(tableName);
		try {
			if (admin.tableExists(table)) {
				admin.truncateTable(table, false);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据rowkey查询 返回结果集Result
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            行健
	 * @return Result 结果集
	 */
	public Result get(String tableName, String rowKey) {
		Table table = null;
		Result result = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			result = table.get(new Get(Bytes.toBytes(rowKey)));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}
		return result;
	}

	/**
	 * 添加数据
	 * 
	 * @param tableName
	 *            表名
	 * @param puts
	 *            Put对象是对Hbase表中一行数据的封装
	 */
	public void put(String tableName, Put... puts) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			for (Put put : puts) {
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 添加数据
	 * 
	 * @param tableName
	 *            表名
	 * @param puts
	 */
	public void put(String tableName, List<Put> puts) {
		put(tableName, puts.toArray(new Put[puts.size()]));
	}

	/**
	 * 删除数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            行健
	 * @param familyName
	 *            列簇名
	 * @param qualifierName
	 *            列名
	 */
	public void delete(String tableName, String rowKey, String familyName,
			String qualifierName) {
		logger.info("delete data from table " + tableName + ".");
		TableName tn = TableName.valueOf(tableName);
		byte[] family = Bytes.toBytes(familyName);
		byte[] qualifier = Bytes.toBytes(qualifierName);
		byte[] row = Bytes.toBytes(rowKey);
		Delete delete = new Delete(row);
		/** 删除某个列的某个版本 **/
		delete.addColumn(family, qualifier);

		/**
		 * 
		 * 删除某个列的所有版本 delete.addColumns(family, qualifier); 删除某个列族
		 * delete.addFamily(family);
		 * 
		 **/

		Table table = null;
		try {
			table = conn.getTable(tn);
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 检索数据-单行获取
	 * 
	 * @param tableName
	 *            表名
	 * @param familyName
	 *            列簇名
	 * @param rowKey
	 *            行健
	 */
	public void get(String tableName, String familyName, String rowKey) {
		Result result = null;
		Table table = null;
		try {
			logger.info("Get data from table " + tableName + " by family "
					+ familyName);
			TableName tn = TableName.valueOf(tableName);
			byte[] family = Bytes.toBytes(familyName);
			byte[] row = Bytes.toBytes(rowKey);
			table = conn.getTable(tn);
			Get get = new Get(row);
			get.addFamily(family);
			result = table.get(get);

			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				String columnFamily = new String(CellUtil.cloneFamily(cell));
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				logger.info(columnFamily + CHARACTER_SEPARATOR + qualifier
						+ CHARACTER_SEPARATOR + value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}

	}

	/**
	 * 全表扫描
	 * 
	 * @param tableName
	 *            表名
	 */
	public void scan(String tableName) {
		scan(tableName, null);
	}

	/**
	 * 只扫描某个列簇
	 * 
	 * @param tableName
	 *            表名
	 * @param familyName
	 *            列簇名
	 */
	public void scan(String tableName, String familyName) {
		logger.info("Scan table " + tableName + " to browse all datas.");
		TableName tn = TableName.valueOf(tableName);
		Scan scan = new Scan();
		if (familyName != null) {
			byte[] family = Bytes.toBytes(familyName);
			scan.addFamily(family);
		}
		Table table = null;
		ResultScanner resultScanner = null;
		try {
			table = conn.getTable(tn);
			resultScanner = table.getScanner(scan);
			for (Iterator<Result> it = resultScanner.iterator(); it
					.hasNext();) {
				Result result = it.next();
				List<Cell> cells = result.listCells();
				for (Cell cell : cells) {
					String columnFamily = new String(
							CellUtil.cloneFamily(cell));
					String qualifier = new String(
							CellUtil.cloneQualifier(cell));
					String value = new String(CellUtil.cloneValue(cell));
					System.out.println(columnFamily + CHARACTER_SEPARATOR
							+ qualifier + CHARACTER_SEPARATOR + value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}
	}

	/** 列出表 **/
	public void listTables() {
		TableName[] names = null;
		try {
			names = admin.listTableNames();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (TableName tableName : names) {
			System.out.println(tableName.getNameAsString());
		}
	}

	/**
	 * 判断表是否存在
	 * 
	 * @param table
	 *            表名
	 * @return boolean true 表示存在,false表示不存在
	 */
	public boolean isExists(String table) {
		TableName tableName = TableName.valueOf(table);

		boolean exists = false;
		try {
			exists = admin.tableExists(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (exists) {
			logger.info("Table " + tableName.getNameAsString()
					+ " already exists.");
		} else {
			logger.info(
					"Table " + tableName.getNameAsString() + " not exists.");
		}
		return exists;
	}

	/** 连接释放,释放所有资源 **/
	public static void closeAll() {
		try {
			if (admin != null) {
				admin.close();
				admin = null;
			}
			if (conn != null) {
				conn.close();
				conn = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 移除协处理器
	 * 
	 * @param tableName
	 *            表名
	 * @param className
	 *            协处理器类名
	 */
	public void removeCoprocessor(String tableName, String className) {
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor hd = new HTableDescriptor(tn);
		hd.removeCoprocessor(className);
	}

	/** 释放表 **/
	private void closeTable(Table table) {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
