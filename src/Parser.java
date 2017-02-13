
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;


public class Parser {
	
	private static Map<String, SeedDataBean> hmMap = new HashMap<String, SeedDataBean>();
	private static Map<String, SeedDataBean> sguidMap = new HashMap<String, SeedDataBean>();
	private static List<SeedDataBean> dupRowKeyList = new ArrayList<SeedDataBean>();
	private static List<SeedDataBean> dupSGUIDList = new ArrayList<SeedDataBean>();
	private static List<SeedDataBean> empSGUIDList = new ArrayList<SeedDataBean>();
	private static boolean isDateEffective = false;
	
	private static Map<String, Map<String, String>> hmSource = new HashMap<String, Map<String, String>>();
	private static Map<String, Map<String, String>> hmTarget = new HashMap<String, Map<String, String>>();
	private static List<String> ignoreList = new ArrayList<String>();
	static {
		ignoreList.add("CreatedBy");
		ignoreList.add("CreationDate");
		ignoreList.add("LastUpdateDate");
		ignoreList.add("LastUpdateLogin");
		ignoreList.add("LastUpdatedBy");
		ignoreList.add("ObjectVersionNumber");
	}
	
	private static Map<String, String> xpathMap = new HashMap<String, String>();
	private static Set<String> noSGUIDSet = new HashSet<String>();
	private static Map<String, String> valueSetMap = new HashMap<String, String>();
	private static Map<String, String> effRowMap = new HashMap<String, String>();
	private static int moved = 0;
	private static int r13missing = 0;
	private static boolean needCompareValueSet = false;
	
	
	public static void main(String[] args) throws Exception {
		String func = args[0];
		if ("parser".equalsIgnoreCase(func)) {
			String filePath = args[1];
			String fileName = getFileName(filePath);
			//String outpuPath = args[1];
			parserSeedDataXML(filePath, fileName);
			generateOutput(fileName, filePath);
			return;
		} 
		if ("compare".equals(func)) {
			String sourceFile = args[1];
			String targetFile = args[2];
			System.out.println("==============================================================================");
			System.out.println("======source file::"+sourceFile);
			System.out.println("======target file::"+targetFile);
			System.out.println("==============================================================================");
			loadSourceFile(sourceFile);
			loadTargetFile(targetFile);
			compareSourceAndTarget();
			return;
		}
		if ("copysguid".equals(func)) {
			String fromFile = args[1];
			String toFile = args[2];
			Map<String, List<String>> ignoreConfig = new HashMap<String, List<String>>();
			if (args.length > 3) {
				if (args[3].startsWith("--valuesetmatch")) {
					needCompareValueSet = true;
				} else {
					String ignoreFile = args[3];
					ignoreConfig = loadMappingFile(ignoreFile);
				}
			}
			System.out.println("==============================================================================");
			System.out.println("======from file::" + fromFile);
			System.out.println("======to file::" + toFile);
			System.out.println("==============================================================================");
			List<String> ignoreColumnList = new ArrayList<String>();
			ignoreColumnList.addAll(ignoreList);
			ignoreColumnList.add("SGUID");
			loadFromFile(fromFile, ignoreColumnList, ignoreConfig);
			copySGUID(toFile, ignoreConfig, ignoreColumnList);
			reformatResultFile(toFile, toFile+".new");
			System.out.println("====================================SUMMARY====================================");
			System.out.println("======SGUID populate to R13::"+moved);
			System.out.println("======Missing R13::"+r13missing);
			System.out.println("==============================================================================");
		}
		
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
		}  catch (Throwable e) {
			e.printStackTrace();
		}
	}

	private static Map<String, List<String>> loadMappingFile(String ignoreFile) throws Exception {
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		if (ignoreFile != null) {
			File file = new File(ignoreFile);
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String result = null;
			while((result = br.readLine()) != null) {
				String[] ignoreList = result.split("::");
				String voName = ignoreList[0];
				String ignoreFields = ignoreList[1];
				if (ignoreFields != null && ignoreFields.length() > 0) {
					String[] ignoreFieldArray = ignoreFields.split(",");
					List<String> ignoreFieldList = Arrays.asList(ignoreFieldArray);
					map.put(voName, ignoreFieldList);
				}
			}
			br.close();
			fr.close();
		}
		return map;
	}
	
	private static void copySGUID(String toFile, Map<String, List<String>> ignoreConfig, List<String> ignoreColumnList) throws Exception {
		SAXReader reader = new SAXReader();
		Map<String, String> map = new HashMap<String, String>();  
		map.put("ns","http://www.oracle.com/apps/fnd/applseed");
		reader.getDocumentFactory().setXPathNamespaceURIs(map);
		Document doc = reader.read(new File(toFile));
		Element eRoot = doc.getRootElement();
		String rootNodeName = eRoot.getName();
		String xPath = "/" + rootNodeName;
		String curPath = xPath;
		String vo = XMLUtil.getNodeAttribute(eRoot, "vo");
		List<Element> lsElement = XMLUtil.findChildNodes(vo, eRoot);
		if (lsElement != null && !lsElement.isEmpty()) {
			for (Element ele : lsElement) {
				String nodeName = ele.getName();
				String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
				String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
				if (debegin != null && "true".equals(debegin)) {
					isDateEffective = true;
				}
				if (rowKey != null) {
					String newXPath = curPath + "/" +  nodeName + "[@rowkey=\"" + rowKey + "\"]";
					String oriXPath = newXPath;
					boolean mapped = false;
					xPath = getXMLNameSpaceFixed(newXPath);
					if (xpathMap.containsKey(newXPath)) {
						mapped = true;
						String sguid = xpathMap.get(newXPath);
						Node node = doc.selectSingleNode(xPath);
						if (node != null) {
							System.out.println("[Existing]"+oriXPath);
							moved++;
							Element sguidNode = XMLUtil.findChildNode("SGUID", node);
							if (sguidNode != null) {
								Attribute isNullAttr = sguidNode.attribute("isNull");
								if (isNullAttr != null) {
									sguidNode.remove(isNullAttr);
								}
								sguidNode.setText(sguid);
							} else {
								createNewSGUIDNode(sguid, node);
							}
						} else {
							if (needFindByAttributeValue(rowKey)) {
								node = XMLUtil.findNodeByAttributeValue("rowkey", rowKey, doc);
								if (node != null) {
									System.out.println("[Existing]"+oriXPath);
									moved++;
									Element sguidNode = XMLUtil.findChildNode("SGUID", node);
									if (sguidNode != null) {
										Attribute isNullAttr = sguidNode.attribute("isNull");
										if (isNullAttr != null) {
											sguidNode.remove(isNullAttr);
										}
										sguidNode.setText(sguid);
									} else {
										createNewSGUIDNode(sguid, node);
									}
								} else {
									System.out.println("[Missing]"+oriXPath);
									r13missing++;
								}
							} else {
								System.out.println("[Missing]"+oriXPath);
								r13missing++;
							}
						}
					} else {
						String valueSetKey = nodeName + "_1_" + generateValueSet(ele, ignoreColumnList, ignoreConfig.get(vo));
						if (valueSetMap.containsKey(valueSetKey) && needCompareValueSet) {
							mapped = true;
							String sguid = valueSetMap.get(valueSetKey);
							Node node = doc.selectSingleNode(xPath);
							if (node != null) {
								System.out.println("[Existing][ValueSet]"+oriXPath);
								moved++;
								Element sguidNode = XMLUtil.findChildNode("SGUID", node);
								if (sguidNode != null) {
									Attribute isNullAttr = sguidNode.attribute("isNull");
									if (isNullAttr != null) {
										sguidNode.remove(isNullAttr);
									}
									sguidNode.setText(sguid);
								} else {
									createNewSGUIDNode(sguid, node);
								}
							} else {
								System.out.println("[Missing][ValueSet]"+oriXPath);
								r13missing++;
							}
						} else if (isDateEffective){
							String effKey = rowKey.substring(0, rowKey.length() - 22);
							if (effRowMap.containsKey(effKey)) {
								mapped = true;
								String sguid = effRowMap.get(effKey);
								Node node = doc.selectSingleNode(xPath);
								if (node != null) {
									System.out.println("[Existing][DateEff]"+oriXPath);
									moved++;
									Element sguidNode = XMLUtil.findChildNode("SGUID", ele);
									if (sguidNode != null) {
										Attribute isNullAttr = sguidNode.attribute("isNull");
										if (isNullAttr != null) {
											sguidNode.remove(isNullAttr);
										}
										sguidNode.setText(sguid);
									} else {
										createNewSGUIDNode(sguid, node);
									}
								} else {
									System.out.println("[Missing][DateEff]"+oriXPath);
									r13missing++;
								}
							}
						} 
					}
					if (!mapped && !noSGUIDSet.contains(curPath + "/" +  nodeName)) {
						System.out.println("[NoMappingForTarget]"+newXPath);
					}
				}
				if (isDateEffective) {
					String deend = XMLUtil.getNodeAttribute(ele, "deend");
					if (deend != null && "true".equals(deend)) {
						isDateEffective = false;
					}
				}
				int index = 1;
				recurseCopySGUID(index, ele,curPath + "/" +  nodeName, ignoreColumnList, doc, ignoreConfig);
			}
		}
		XMLWriter writer = new XMLWriter(new FileOutputStream(new File(toFile+".new")));
        writer.write(doc);
        writer.close();
	}

	private static boolean needFindByAttributeValue(String rowKey) {
		return rowKey.contains("\\.") || rowKey.contains("/oldValue") || rowKey.contains("/newValue") || rowKey.contains("http://xmlns") || rowKey.contains("${");
	}
	
	public static String getXMLNameSpaceFixed(String xpath) {  
	    xpath= xpath.replaceAll("/(\\w)", "/"+"ns:$1");//replace start with "/"  
	    xpath= xpath.replaceAll("^(\\w)", "ns:$1");    //replace start with word  
	    return xpath;  
	}  

	private static void loadFromFile(String fromFile, List<String> ignoreColumnList, Map<String, List<String>> ignoreList) throws Exception {
		Document doc = XMLUtil.xmlFileToDom(fromFile);
		Element eRoot = doc.getRootElement();
		String rootNodeName = eRoot.getName();
		String xPath = "/" + rootNodeName;
		String vo = XMLUtil.getNodeAttribute(eRoot, "vo");
		List<Element> lsElement = XMLUtil.findChildNodes(vo, eRoot);
		if (lsElement != null && !lsElement.isEmpty()) {
			for (Element ele : lsElement) {
				String nodeName = ele.getName();
				String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
				boolean hasSGUID = hasSGUIDNode(ele);
				String SGUID = XMLUtil.getChildNodeValue(ele, "SGUID", true);
				if (hasSGUID && rowKey != null) {
					String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
					if (debegin != null && "true".equals(debegin)) {
						isDateEffective = true;
					}
					if (isDateEffective) {
						String effKey = rowKey.substring(0, rowKey.length() - 22);
						effRowMap.put(effKey, SGUID);
					}
					String newXPath = xPath + "/" +  nodeName + "[@rowkey=\"" + rowKey + "\"]";
					xpathMap.put(newXPath, SGUID);
					String valueSetKey = nodeName + "_1_" + generateValueSet(ele, ignoreColumnList, ignoreList.get(vo));
					valueSetMap.put(valueSetKey, SGUID);
					if (isDateEffective) {
						String deend = XMLUtil.getNodeAttribute(ele, "deend");
						if (deend != null && "true".equals(deend)) {
							isDateEffective = false;
						}
					}
				} else {
					noSGUIDSet.add(xPath + "/" +  nodeName);
				}
				int index = 1;
				recurseProcessSameChildNodesForCopySGUID(index, ele,xPath + "/" +  nodeName, ignoreColumnList, ignoreList);
			}
		}
	}
	
	private static void recurseProcessSameChildNodesForCopySGUID(int index, Element nPrarent, String xPath, List<String> ignoreColumnList, Map<String, List<String>> ignoreList) {
		index ++;
		List<Element> lsElement =  XMLUtil.findAllChildNodes(nPrarent);
		if (lsElement != null && !lsElement.isEmpty()) {
			for (Element ele : lsElement) {
				String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
				if (rowKey == null || rowKey.length() == 0) {
					continue;
				}
				String nodeName = ele.getName();
				boolean hasSGUID = hasSGUIDNode(ele);
				String SGUID = XMLUtil.getChildNodeValue(ele, "SGUID", true);
				if (hasSGUID) {
					String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
					if (debegin != null && "true".equals(debegin)) {
						isDateEffective = true;
					}
					if (isDateEffective) {
						String effKey = rowKey.substring(0, rowKey.length() - 22);
						effRowMap.put(effKey, SGUID);
					}
					String newXPath = xPath + "/" +  nodeName + "[@rowkey=\"" + rowKey + "\"]";
					xpathMap.put(newXPath, SGUID);
					String valueSetKey = nodeName + "_"+index+"_" + generateValueSet(ele, ignoreColumnList, ignoreList.get(nodeName));
					valueSetMap.put(valueSetKey, SGUID);
					if (isDateEffective) {
						String deend = XMLUtil.getNodeAttribute(ele, "deend");
						if (deend != null && "true".equals(deend)) {
							isDateEffective = false;
						}
					}
				} else {
					noSGUIDSet.add(xPath + "/" +  nodeName);
				}
				recurseProcessSameChildNodesForCopySGUID(index, ele, xPath + "/" +  nodeName, ignoreColumnList, ignoreList);
			}
		}
	}
	
	private static void recurseCopySGUID(int index, Element nPrarent, String xPath, List<String> ignoreColumnList, Document doc, Map<String, List<String>> ignoreConfig) {
		index ++;
		String curPath = xPath;
		List<Element> lsElement =  XMLUtil.findAllChildNodes(nPrarent);
		if (lsElement != null && !lsElement.isEmpty()) {
			for (Element ele : lsElement) {
				String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
				if (rowKey == null || rowKey.length() == 0) {
					continue;
				}
				String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
				if (debegin != null && "true".equals(debegin)) {
					isDateEffective = true;
				}
				String nodeName = ele.getName();
				String newXPath = curPath + "/" +  nodeName + "[@rowkey=\"" + rowKey + "\"]";
				String oriXPath = newXPath;
				xPath = getXMLNameSpaceFixed(newXPath);
				boolean mapped = false;
				if (xpathMap.containsKey(newXPath)) {
					mapped = true;
					String sguid = xpathMap.get(newXPath);
					Node node = doc.selectSingleNode(xPath);
					if (node != null) {
						System.out.println("[Existing]"+oriXPath);
						moved++;
						Element sguidNode = XMLUtil.findChildNode("SGUID", node);
						if (sguidNode != null) {
							Attribute isNullAttr = sguidNode.attribute("isNull");
							if (isNullAttr != null) {
								sguidNode.remove(isNullAttr);
							}
							sguidNode.setText(sguid);
						} else {
							createNewSGUIDNode(sguid, node);
						}
					} else {
						if (needFindByAttributeValue(rowKey)) {
							node = XMLUtil.findNodeByAttributeValue("rowkey", rowKey, doc);
							if (node != null) {
								System.out.println("[Existing]"+oriXPath);
								moved++;
								Element sguidNode = XMLUtil.findChildNode("SGUID", node);
								if (sguidNode != null) {
									Attribute isNullAttr = sguidNode.attribute("isNull");
									if (isNullAttr != null) {
										sguidNode.remove(isNullAttr);
									}
									sguidNode.setText(sguid);
								} else {
									createNewSGUIDNode(sguid, node);
								}
							} else {
								System.out.println("[Missing]"+oriXPath);
								r13missing++;
							}
						} else {
							System.out.println("[Missing]"+oriXPath);
							r13missing++;
						}
					}
				} else {
					String valueSetKey = nodeName + "_"+index+"_" + generateValueSet(ele, ignoreColumnList, ignoreConfig.get(nodeName));
					if (valueSetMap.containsKey(valueSetKey) && needCompareValueSet) {
						mapped = true;
						String sguid = valueSetMap.get(valueSetKey);
						Node node = doc.selectSingleNode(xPath);
						if (node != null) {
							System.out.println("[Existing][ValueSet]"+oriXPath);
							moved++;
							Element sguidNode = XMLUtil.findChildNode("SGUID", node);
							if (sguidNode != null) {
								Attribute isNullAttr = sguidNode.attribute("isNull");
								if (isNullAttr != null) {
									sguidNode.remove(isNullAttr);
								}
								sguidNode.setText(sguid);
							} else {
								createNewSGUIDNode(sguid, node);
							}
						} else {
							System.out.println("[Missing][ValueSet]"+oriXPath);
							r13missing++;
						}
					} else if (isDateEffective) {
						String effKey = rowKey.substring(0, rowKey.length() - 22);
						if (effRowMap.containsKey(effKey)) {
							mapped = true;
							String sguid = effRowMap.get(effKey);
							Node node = doc.selectSingleNode(xPath);
							if (node != null) {
								System.out.println("[Existing][DateEff]"+oriXPath);
								moved++;
								Element sguidNode = XMLUtil.findChildNode("SGUID", ele);
								if (sguidNode != null) {
									Attribute isNullAttr = sguidNode.attribute("isNull");
									if (isNullAttr != null) {
										sguidNode.remove(isNullAttr);
									}
									sguidNode.setText(sguid);
								} else {
									createNewSGUIDNode(sguid, node);
								}
							} else {
								System.out.println("[Missing][DateEff]"+oriXPath);
								r13missing++;
							}
						}
					}
					if (!mapped && !noSGUIDSet.contains(curPath + "/" +  nodeName)) {
						System.out.println("[NoMappingForTarget]"+newXPath);
					}
				}
				if (isDateEffective) {
					String deend = XMLUtil.getNodeAttribute(ele, "deend");
					if (deend != null && "true".equals(deend)) {
						isDateEffective = false;
					}
				}
				recurseCopySGUID(index, ele, curPath + "/" +  nodeName, ignoreColumnList, doc, ignoreConfig);
			}
		}
	}

	private static void createNewSGUIDNode(String sguid, Node voNode) {
		Element sguidEle = ((Element)voNode).addElement("SGUID");
		sguidEle.addText(sguid);
		/*
		Element SGUIDElement = DocumentHelper.createElement("SGUID");
		SGUIDElement.setText(sguid);
		((Element)node).add(SGUIDElement);
		*/
	}
	
	private static String generateValueSet(Element ele, List<String> ignoreElementList, List<String> extIgnoreList) {
		String key = "";
		List<Element> nodeList = XMLUtil.findAllChildNodes(ele);
		if (nodeList != null && !nodeList.isEmpty()) {
			for (Element node : nodeList) {
				String ndName = node.getName();
				if (ignoreElementList.contains(ndName)) {
					continue;
				}
				String ndValue = XMLUtil.getNodeValue(node, true);
				if (ndValue != null) {
					key = key + "::" + ndValue;
				}
			}
		}
		return key;
	}

	private static void compareSourceAndTarget() {
		Set<String> allList = mergeKey();
		if (!allList.isEmpty()) {
			Iterator<String> allIter = allList.iterator();
			while(allIter.hasNext()) {
				String key = allIter.next();
				if (!hmSource.containsKey(key)) {
					System.out.println("[rowKeyMissing]"+key + " existing in target but not in source");
				} else if (!hmTarget.containsKey(key)) {
					System.out.println("[rowKeyMissing]"+key + " existing in source but not in target");
				} else if (hmSource.containsKey(key) && hmTarget.containsKey(key)) {
					Map<String, String> sourceMap = hmSource.get(key);
					Map<String, String> targetMap = hmTarget.get(key);
					
					Set<String> subAllList = mergeSubKey(sourceMap, targetMap);
					Iterator<String> subAllIter = subAllList.iterator();
					while(subAllIter.hasNext()) {
						String subKey = subAllIter.next();
						if (!sourceMap.containsKey(subKey)) {
							System.out.println("[AttrMissing]"+key + "::"+subKey+" existing in target but not in source");
						} else if (!targetMap.containsKey(subKey)) {
							System.out.println("[AttrMissing]"+key + "::"+subKey+ " existing in source but not in target");
						} else {
							String sourceValue = sourceMap.get(subKey);
							String targetValue = targetMap.get(subKey);
							if (!sourceValue.equals(targetValue)) {
								System.out.println("[AttrDiff]"+key + "::"+subKey+" has different value :: source value::"+sourceValue+"::target value::"+targetValue);
							}
						}
					}
				}
			}
		}
	}

	private static Set<String> mergeSubKey(Map<String, String> sourceMap, Map<String, String> targetMap) {
		Set<String> set = new HashSet<String>();
		set.addAll(sourceMap.keySet());
		set.addAll(targetMap.keySet());
		return set;
	}

	private static Set<String> mergeKey() {
		Set<String> set = new HashSet<String>();
		set.addAll(hmSource.keySet());
		set.addAll(hmTarget.keySet());
		return set;
	}

	private static void loadTargetFile(String targetFile) throws Exception {
		Document doc = XMLUtil.xmlFileToDom(targetFile);
		Element root = doc.getRootElement();
		if (root != null && "SEEDDATA".equalsIgnoreCase(root.getName())) {
			List<Element> lsElement = XMLUtil.findAllChildNodes(root);
			if (lsElement != null && !lsElement.isEmpty()) {
				for (Element ele : lsElement) {
					String nodeName = ele.getName();
					String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
					if (rowKey == null || rowKey.length() == 0) {
						rowKey = "empty::"+UUID.randomUUID();
					}
					String key = nodeName + "_1_" + rowKey;
					Map<String, String> map = new HashMap<String, String>();
					List<Element> nodeList = XMLUtil.findAllChildNodes(ele);
					if (nodeList != null && !nodeList.isEmpty()) {
						for (Element node : nodeList) {
							String ndName = node.getName();
							if (ignoreList.contains(ndName)) {
								continue;
							}
							String ndRowKey = XMLUtil.getNodeAttribute(node, "rowkey");
							if (ndRowKey != null && ndRowKey.length() > 0) {
								recurseLoadChildNode(node, 1, ndRowKey, ndName, hmTarget);
							} else {
								String ndValue = XMLUtil.getNodeValue(node, true);
								map.put(ndName, ndValue);
							}
						}
					}
					hmTarget.put(key, map);
				}
			}
		}
		
	}

	private static void loadSourceFile(String sourceFile)  throws Exception {
		Document doc = XMLUtil.xmlFileToDom(sourceFile);
		Element root = doc.getRootElement();
		if (root != null && "SEEDDATA".equalsIgnoreCase(root.getName())) {
			List<Element> lsElement = XMLUtil.findAllChildNodes(root);
			if (lsElement != null && !lsElement.isEmpty()) {
				for (Element ele : lsElement) {
					String nodeName = ele.getName();
					String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
					if (rowKey == null || rowKey.length() == 0) {
						rowKey = "empty::"+UUID.randomUUID();
					}
					String key = nodeName + "_1_" + rowKey;
					Map<String, String> map = new HashMap<String, String>();
					List<Element> nodeList = XMLUtil.findAllChildNodes(ele);
					if (nodeList != null && !nodeList.isEmpty()) {
						for (Element node : nodeList) {
							String ndName = node.getName();
							if (ignoreList.contains(ndName)) {
								continue;
							}
							String ndRowKey = XMLUtil.getNodeAttribute(node, "rowkey");
							if (ndRowKey != null && ndRowKey.length() > 0) {
								recurseLoadChildNode(node, 1, ndRowKey, ndName, hmSource);
							} else {
								String ndValue = XMLUtil.getNodeValue(node, true);
								map.put(ndName, ndValue);
							}
						}
					}
					hmSource.put(key, map);
				}
			}
		}
	}
	
	private static void recurseLoadChildNode(Element ele, int index, String rowKey, String nodeName, Map<String, Map<String, String>> map) {
		index ++;
		String key = nodeName + "_"+index+"_" + rowKey;
		Map<String, String> hmValues = new HashMap<String, String>();
		List<Element> nodeList = XMLUtil.findAllChildNodes(ele);
		if (nodeList != null && !nodeList.isEmpty()) {
			for (Element node : nodeList) {
				String ndName = node.getName();
				if (ignoreList.contains(ndName)) {
					continue;
				}
				String ndRowKey = XMLUtil.getNodeAttribute(node, "rowkey");
				if (ndRowKey != null && ndRowKey.length() > 0) {
					recurseLoadChildNode(node, index, ndRowKey, ndName, map);
				} else {
					String ndValue = XMLUtil.getNodeValue(node, true);
					hmValues.put(ndName, ndValue);
				}
			}
		}
		map.put(key, hmValues);
	}

	private static String getFileName(String filePath) {
		if (filePath != null) {
			int index = filePath.lastIndexOf(File.separator);
			String fileName = filePath.substring(index+1, filePath.length());
			return fileName;
		} else {
			return "";
		}
			
	}

	private static void generateOutput(String fileName, String filePath) throws Exception {
		File file = new File("detail.log");
		FileWriter fw = new FileWriter(file, true);
		fw.write("begin====="+filePath+"=====\r\n");
		if (!dupRowKeyList.isEmpty()) {
			fw.write("=====dup row key list====="+"\r\n");
			for (SeedDataBean bean : dupRowKeyList) {
				fw.write(bean.toString()+"\r\n");
			}
		}
		if (!dupSGUIDList.isEmpty()) {
			fw.write("=====dup SGUID list====="+"\r\n");
			for (SeedDataBean bean : dupSGUIDList) {
				fw.write(bean.toString()+"\r\n");
			}
		}
		if (!empSGUIDList.isEmpty()) {
			fw.write("=====empty SGUID list====="+"\r\n");
			for (SeedDataBean bean : empSGUIDList) {
				fw.write(bean.toString()+"\r\n");
			}
		}
		fw.write("end====="+filePath+"=====\r\n\r\n");
		fw.flush();
		fw.close();
		
		File log = new File("overview.log");
		FileWriter fw2 = new FileWriter(log, true);
		fw2.append(filePath).append(",");
		
		if (!dupRowKeyList.isEmpty()) {
			fw2.append("1,");
		} else {
			fw2.append("0,");
		}
		if (!dupSGUIDList.isEmpty()) {
			fw2.append("1,");
		} else {
			fw2.append("0,");
		}
		if (!empSGUIDList.isEmpty()) {
			fw2.append("1,");
		} else {
			fw2.append("0,");
		}
		fw2.append("\r\n");
		fw2.flush();
		fw2.close();
	}

	public static void parserSeedDataXML(String filePath, String fileName) throws Exception {
		Document doc = XMLUtil.xmlFileToDom(filePath);
		Element eRoot = doc.getRootElement();
		String vo = XMLUtil.getNodeAttribute(eRoot, "vo");
		List<Element> lsElement = XMLUtil.findChildNodes(vo, eRoot);
		if (lsElement != null && !lsElement.isEmpty()) {
			for (Element ele : lsElement) {
				String nodeName = ele.getName();
				String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
				boolean hasSGUID = hasSGUIDNode(ele);
				String SGUID = XMLUtil.getChildNodeValue(ele, "SGUID", true);
				SeedDataBean bean = new SeedDataBean(fileName, vo, 1, rowKey, SGUID);
				if (hmMap.containsKey(nodeName+"_"+rowKey)) {
					dupRowKeyList.add(bean);
				} else {
					hmMap.put(nodeName+"_"+rowKey, bean);
				}
				if (hasSGUID && (SGUID == null || SGUID.length() == 0)) {
					empSGUIDList.add(bean);
				} else {
					if (hasSGUID) {
						String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
						if (debegin != null && "true".equals(debegin)) {
							isDateEffective = true;
						}
						if (isDateEffective && sguidMap.containsKey(nodeName+"_"+SGUID)) {
							//dupSGUIDList.add(bean);
						} else {
							if (!isDateEffective && sguidMap.containsKey(nodeName+"_"+SGUID)) {
								dupSGUIDList.add(bean);
							} else {
								sguidMap.put(nodeName+"_"+SGUID, bean);
							}
						}
						if (isDateEffective) {
							String deend = XMLUtil.getNodeAttribute(ele, "deend");
							if (deend != null && "true".equals(deend)) {
								isDateEffective = false;
							}
						}
					}
				}
				int index = 1;
				recurseProcessSameChildNodes(fileName, ele,index);
			}
		}
	}
	
	private static void recurseProcessSameChildNodes(String fileName,Element nPrarent, int index) {
		index++;
		List<Element> lsElement =  XMLUtil.findAllChildNodes(nPrarent);
		if (lsElement != null && !lsElement.isEmpty()) {
			for (Element ele : lsElement) {
				String rowKey = XMLUtil.getNodeAttribute(ele, "rowkey");
				if (rowKey == null || rowKey.length() == 0) {
					continue;
				}
				String nodeName = ele.getName();
				boolean hasSGUID = hasSGUIDNode(ele);
				String SGUID = XMLUtil.getChildNodeValue(ele, "SGUID", true);
				SeedDataBean bean = new SeedDataBean(fileName, ele.getName(), index, rowKey, SGUID);
				if (hmMap.containsKey(nodeName+"_"+rowKey)) {
					dupRowKeyList.add(bean);
				} else {
					hmMap.put(nodeName+"_"+rowKey, bean);
				}
				if (hasSGUID && (SGUID == null || SGUID.length() == 0)) {
					empSGUIDList.add(bean);
				} else {
					if (hasSGUID) {
						String debegin = XMLUtil.getNodeAttribute(ele, "debegin");
						if (debegin != null && "true".equals(debegin)) {
							isDateEffective = true;
						}
						if (isDateEffective && sguidMap.containsKey(nodeName+"_"+SGUID)) {
							//dupSGUIDList.add(bean);
						} else {
							if (!isDateEffective && sguidMap.containsKey(nodeName+"_"+SGUID)) {
								dupSGUIDList.add(bean);
							} else {
								sguidMap.put(nodeName+"_"+SGUID, bean);
							}
						}
						if (isDateEffective) {
							String deend = XMLUtil.getNodeAttribute(ele, "deend");
							if (deend != null && "true".equals(deend)) {
								isDateEffective = false;
							}
						}
					}
				}
				recurseProcessSameChildNodes(fileName,ele,index);
			}
		}
	}

	private static boolean hasSGUIDNode(Element ele) {
		return XMLUtil.findChildNode("SGUID", ele) != null;
	}

	
}

