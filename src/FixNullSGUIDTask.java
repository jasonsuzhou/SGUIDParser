import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

public class FixNullSGUIDTask {

	private static boolean isDateEffective = false;
	private static String deSGUID = null;

	public static void processTask(String fileName, String dburl, boolean needEscape) {
		Connection conn = JDBCUtils.getConn(dburl, "fusion", "fusion");
		BufferedReader br = null;
		FileReader reader = null;
		// StringBuilder sb = new StringBuilder(10240);
		try {
			reader = new FileReader(fileName);
			br = new BufferedReader(reader);
			String str = null;
			while ((str = br.readLine()) != null) {
				final String line = str;

				Document doc = XMLUtil.xmlFileToDom(line);
				Element eRoot = doc.getRootElement();
				String vo = XMLUtil.getNodeAttribute(eRoot, "vo");
				List<Element> lsElement = XMLUtil.findChildNodes(vo, eRoot);
				if (lsElement != null && !lsElement.isEmpty()) {
					for (Element ele : lsElement) {
						String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
						String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
						if (debegin != null && "true".equals(debegin)) {
							isDateEffective = true;
							deSGUID = generateSGUID(conn);
						}
						if (rowKey != null) {
							Element sguidNode = XMLUtil.findChildNode("SGUID", ele);
							if (sguidNode != null) {
								Attribute isNullAttr = sguidNode.attribute("isNull");
								if (isNullAttr != null) {
									sguidNode.remove(isNullAttr);
								}
								String value = XMLUtil.getNodeValue(sguidNode, true);
								if (value == null || "".equals(value)) {
									if (isDateEffective) {
										sguidNode.setText(deSGUID);
										System.out.println(line + ".fixnull" + "|" + rowKey + "|" + deSGUID);
									} else {
										String sguidValue = generateSGUID(conn);
										sguidNode.setText(sguidValue);
										System.out.println(line + ".fixnull" + "|" + rowKey + "|" + sguidValue);
									}
								}
							}
						}
						if (isDateEffective) {
							String deend = XMLUtil.getNodeAttribute(ele, "deend");
							if (deend != null && "true".equals(deend)) {
								isDateEffective = false;
								deSGUID = null;
							}
						}

						int index = 1;
						recurseFixChildNullSGUID(index, ele, conn, line);
					}
				}
				OutputFormat format = new OutputFormat();
				format.setExpandEmptyElements(true);
				XMLWriter writer = new XMLWriter(new FileOutputStream(new File(line + ".fixnull")), format);
				writer.setEscapeText(needEscape);
				writer.write(doc);
				writer.close();

				reformatResultFile(line, line + ".fixnull");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			JDBCUtils.closeConn(conn);
		}
	}

	private static void recurseFixChildNullSGUID(int index, Element nPrarent, Connection conn, String line)
			throws Exception {
		boolean isDateEffective = false;
		String deSGUID = null;
		index++;
		List<Element> lsElement = XMLUtil.findAllChildNodes(nPrarent);
		if (lsElement != null && !lsElement.isEmpty()) {
			for (Element ele : lsElement) {
				String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
				if (rowKey == null || rowKey.length() == 0) {
					continue;
				}
				String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
				if (debegin != null && "true".equals(debegin)) {
					isDateEffective = true;
					deSGUID = generateSGUID(conn);
				}
				Element sguidNode = XMLUtil.findChildNode("SGUID", ele);
				if (sguidNode != null) {
					Attribute isNullAttr = sguidNode.attribute("isNull");
					if (isNullAttr != null) {
						sguidNode.remove(isNullAttr);
					}
					String value = XMLUtil.getNodeValue(sguidNode, true);
					if (value == null || "".equals(value)) {
						if (isDateEffective) {
							sguidNode.setText(deSGUID);
							System.out.println(line + ".fixnull" + "|" + rowKey + "|" + deSGUID);
						} else {
							String sguidValue = generateSGUID(conn);
							sguidNode.setText(sguidValue);
							System.out.println(line + ".fixnull" + "|" + rowKey + "|" + sguidValue);
						}
					}
				}
				if (isDateEffective) {
					String deend = XMLUtil.getNodeAttribute(ele, "deend");
					if (deend != null && "true".equals(deend)) {
						isDateEffective = false;
						deSGUID = null;
					}
				}
				recurseFixChildNullSGUID(index, ele, conn, line);
			}
		}
	}

	private static String generateSGUID(Connection conn) throws SQLException {
		Statement stmt;
		ResultSet rs;
		String result = null;
		String sql = "select SYS_GUID() as sguid from dual";
		stmt = conn.createStatement();
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			result = rs.getString("sguid");
			break;
		}
		JDBCUtils.closeRs(rs);
		JDBCUtils.closeStmt(stmt);
		return result;
	}

	private static void reformatResultFile(String toFile, String targetFile) {
		try {
			StringBuffer sb1 = new StringBuffer();
			FileUtils.readToBuffer(sb1, toFile);
			String oriDocString = sb1.toString();

			StringBuffer sb2 = new StringBuffer();
			FileUtils.readToBuffer(sb2, targetFile);
			String targetDocString = sb2.toString();
			int index1 = oriDocString.indexOf("<SEEDDATA ");
			int index2 = targetDocString.indexOf("<SEEDDATA ");
			String needReplaceString = oriDocString.substring(0, index1);
			targetDocString = targetDocString.substring(index2, targetDocString.length());
			targetDocString = needReplaceString + targetDocString;
			File file = new File(targetFile);
			FileWriter fw = new FileWriter(file);
			fw.write(targetDocString);
			fw.flush();
			fw.close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}
