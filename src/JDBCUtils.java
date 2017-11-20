import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCUtils {

	public static Connection getConn(String host, String port, String sid, String username, String password) {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection connection = null;
			connection = DriverManager.getConnection("jdbc:oracle:thin:@" + host + ":" + port + ":" + sid, username,
					password);
			return connection;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Connection getConn(String url, String username, String password) {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection connection = null;
			connection = DriverManager.getConnection("jdbc:oracle:thin:@"+url, username, password);
			return connection;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void closeConn(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void closeStmt(Statement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void closeRs(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void closeAll(Connection conn, Statement stmt, ResultSet rs) {
		closeRs(rs);
		closeStmt(stmt);
		closeConn(conn);
	}
	
	public static String generateSGUID(String url, String username, String password) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = getConn(url, username, password);
			if (conn != null) {
				String sql = "select SYS_GUID() as sguid from dual";
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					return rs.getString("sguid");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeAll(conn, stmt, rs);
		}
		return null;
	}

}
