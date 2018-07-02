package com.liuyang.xdr.udf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.liuyang.data.util.Row;
import com.liuyang.data.util.Schema;
import com.liuyang.log.Logger;

public class Meta {
	//private final static Logger logger = Logger.getLogger(Meta.class);
	
	private static final String MYSQL_URI = "jdbc:mysql://master:3306/xdrfile?useUnicode=true&characterEncoding=UTF-8&cachePrepStmts=true";
	private static final String MYSQL_USER = "lcxdrchecker";
	private static final String MYSQL_PASS = "lcservis@mysql2017";
	/**
	 * XDRFILE 查询文件信息语句模版
	 * @param DATESTAMP
	 * @param TABLEID
	 * @param HOURS
	 * @param SATAUS
	 */
	private static final String XDRFILE_QUERY_FILEINFO = "SELECT * FROM xdrfile.file_list_%s WHERE indirect_table_id in (%s) AND file_create_hour in (%s) AND receiver_file_status in (%s)";
	/**
	 * XDRFILE 更新文件接收信息模版
	 * @param DATESTAMP
	 * @param FILELINES  文件数据条数
	 * @param ENDTIME 接收结束时间
	 * @param FILELENGTH 文件接收长度
	 * @param STATUS 文件状态
	 * @param FILENAME 文件名称
	 */
	private static final String XDRFILE_UPDATE_RECEIVE = "UPDATE xdrfile.file_list_%s SET file_lines = %s, receiver_end_time = %s, receiver_file_length = %s, receiver_file_status = %s WHERE file_name ='%s'";
	/**
	 * @param DATESTAMP
	 * @param START_TIME
	 * @param STATUS
	 * @param FILENAME
	 */
	private static final String XDRFILE_UPDATE_STATUS = "UPDATE xdrfile.file_list_%s SET receiver_start_time = %s, receiver_file_status = %s WHERE file_name ='%s'";
	
	private static final String XDRCONFIG_TABLE_SCHEMA = "SELECT * FROM xdrconfig.xdr_table_struct t WHERE table_id = ?";
	
	private final static MySQLManager getMySQLConnection() {
		return new MySQLManager(MYSQL_URI, MYSQL_USER, MYSQL_PASS);
	}
	
	public final static List<Row> select(String inSqlStr) {
		MySQLManager mysql = null;
		List<Row> retval = null;
		System.out.println(inSqlStr);
		try {
			mysql = getMySQLConnection();
			if (mysql.isContected()) {
				retval = mysql.select(inSqlStr);
			}
			mysql.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
            mysql = null;
		}
		return retval;
	}
	
	public static int update(String inSqlStr) {
		MySQLManager mysql = null;
		int retval = -1;
		System.out.println(inSqlStr);
		try {
			mysql = getMySQLConnection();
			if (mysql.isContected()) {
				retval = mysql.update(inSqlStr);
			}
			mysql.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			mysql = null;
		}
		return retval;
	}
	
	public static Row getOneXDRFile(String datestamp, String tableIds, String hours, String status) {
		MySQLManager mysql = null;
		List<Row> result = null;
		Row retval = null;
		try {
			mysql = getMySQLConnection();
			if (mysql.isContected()) {
				result = mysql.select(String.format(XDRFILE_QUERY_FILEINFO, datestamp, tableIds, hours, status) + " limit 1");
				if (result.size() > 0) {
					retval = result.get(0);
					// 将文件状态设定为1，使该文件不会再被选取
					mysql.update(String.format(XDRFILE_UPDATE_STATUS, datestamp, System.currentTimeMillis(), 1, retval.get("file_name")));
				}
			}
			mysql.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
            mysql = null;
		}
		return retval;
	}
	
	public static int updateXDRFile(String datestamp, String fileName, String lines, String endTime, String length, String status) {
		MySQLManager mysql = null;
		int retval = 0;
		try {
			mysql = getMySQLConnection();
			if (mysql.isContected()) {
				retval = mysql.update(String.format(XDRFILE_UPDATE_RECEIVE
						,datestamp
						,lines
						,endTime
						,length
						,status
						,fileName
					));
				mysql.close();
			}			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
            mysql = null;
		}
		return retval;
	}
	
	public synchronized static Schema getSchema(String tableId) {
		MySQLManager mysql = null;
		List<Row> result = null;
		Schema retval = null;
		try {
			mysql = getMySQLConnection();
			if (mysql.isContected()) {
				result = mysql.select(XDRCONFIG_TABLE_SCHEMA, tableId);
				
				if (result.size() > 0) {
					retval = Schema.createStruct(String.valueOf(result.get(0).get("table_name")));
					for (Row row : result) {
						retval.addField(Schema.create(
								row.getString("column_name")
							   ,row.getString("column_type")
						));
					}
				}
			}
			mysql.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
            mysql = null;
		}
		return retval;
	}
	
	public static class MySQLManager {
		
		private String url, user, pass;
		
		private Connection conn;
		
		public MySQLManager(String url, String user, String pass) {
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(url, user, pass);
				this.url = url;
				this.user = user;
				this.pass = pass;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		private synchronized final boolean tryConnect() throws SQLException {
			boolean retval = false;
			if (!(retval = isContected())) {
				conn = DriverManager.getConnection(url, user, pass);
				retval = isContected();
			}
			return retval;
		}
		
		@Override
		protected void finalize() {
			close();
			url = null;
			user = null;
			pass = null;
			conn = null;
		}
		
		public synchronized boolean isContected() throws SQLException {
			if (conn == null) return false;
			return conn.isValid(3000);
		}
		
		private synchronized final Schema getSchema(String tableName, ResultSetMetaData rsmd) throws SQLException {
			Schema retval = Schema.createStruct(tableName);
			for (int i = 1, length = rsmd.getColumnCount(); i <= length; i++) {
				retval.addField(Schema.create(rsmd.getColumnLabel(i), rsmd.getColumnTypeName(i)));
			}
			return retval;
		}
		
		public synchronized final List<Row> select(String inSqlStr, Object... parameters) {
			List<Row> retval = null;
			//System.out.println(inSqlStr);
			try {
				if (tryConnect()) {
					retval = new ArrayList<Row>();
					PreparedStatement pstm = conn.prepareStatement(inSqlStr);
					if (parameters != null) {
						for(int i = 1, length = parameters.length; i <= length; i++) {
							pstm.setObject(i, parameters[i - 1]);
						}
					}
					ResultSet rs = pstm.executeQuery();
					Schema schema = null;
					while(rs.next()) {
						ResultSetMetaData rsmd = rs.getMetaData();
						if (schema == null) schema = getSchema(inSqlStr.substring(0, 10), rsmd);
						//Map<String, Object> row = new LinkedHashMap<String, Object>();
						Row row = schema.createRow();
						for (int i = 1, length = rsmd.getColumnCount(); i <= length; i++) {
							row.setValue(rsmd.getColumnLabel(i), rs.getObject(i));
							//row.put(rsmd.getColumnLabel(i), rs.getObject(i));
						}
						retval.add(row);
						rsmd = null;
					}
					rs.close();
					pstm.close();
					pstm = null;
					rs = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				
			}
			return retval;
		}
		
		public synchronized final List<Row> select(String inSqlStr) {
			return select(inSqlStr, new Object[] {});
		}
		

		
		public synchronized int update(String inSqlStr) {
			int retval = -1;
			//System.out.println(inSqlStr);
			try {
				if (tryConnect()) {
					PreparedStatement pstm = conn.prepareStatement(inSqlStr);
					retval = pstm.executeUpdate();
					pstm = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				
			}
			return retval;
		}
		
		public synchronized void close() {
			try {
				if (conn != null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				conn = null;
			}
		}
	}
}
