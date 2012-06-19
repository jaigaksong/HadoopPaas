package com.sap.hadoop.master;

import java.util.ArrayList;
import java.util.List;

public class MasterConfig {
	private static final String HD_URI = "hdfs://%s:9000/PAAS/lib/%s";
	private String command;
	private List<Resource> resources = new ArrayList<Resource>();
	private int memory = 2048;
	private String serviceName;
	private String hadoopHost;
	private String zkHostPort;	
	
	public MasterConfig(String serviceName, int memSize, String hadoopHost, String zkHostPort) {
		this.serviceName = serviceName;
		this.memory = memSize;
		this.hadoopHost = hadoopHost;
		this.zkHostPort = zkHostPort;
		this.command = String.format("${JAVA_HOME}/bin/java com.sap.hadoop.client.PaasAppContainer %s %s %s", serviceName, hadoopHost, zkHostPort);
		initResources();
	}
	
	public String getCommand() {
		return command;
	}
	
	public void setCommand(String command) {
		this.command = command;
	}
	
	public List<Resource> getResources() {
		return resources;
	}
	
	public void setResources(List<Resource> resources) {
		this.resources = resources;
	}
	
	public int getMemory() {
		return memory;
	}
	
	public void setMemory(int memory) {
		this.memory = memory;
	}
		
	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getHadoopHost() {
		return hadoopHost;
	}

	public void setHadoopHost(String hadoopHost) {
		this.hadoopHost = hadoopHost;
	}

	public String getZkHostPort() {
		return zkHostPort;
	}

	public void setZkHostPort(String zkHostPort) {
		this.zkHostPort = zkHostPort;
	}

	public class Resource {
		public String name;
		public String path;
		
		public Resource(String name, String path) {
			this.name = name;
			this.path = path;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getPath() {
			return path;
		}
		
		public void setPath(String path) {
			this.path = path;
		}
	}
	
	private void initResources() {
		String jarNames[] = {"PaasAppContainer.jar", 
				"ant-1.6.5.jar",				
				"javax.servlet.jsp-2.2.0.jar",
				"jetty-server-8.1.4.jar", 
				"jetty-util-8.1.4.jar", 
				"servlet-api-3.0.jar", 
				"jetty-webapp-8.1.4.jar",
				"jetty-servlet-8.1.4.jar", 
				"jetty-http-8.1.4.jar", 
				"jetty-io-8.1.4.jar", 
				"jetty-security-8.1.4.jar", 
				"jetty-xml-8.1.4.jar",
				"jetty-continuation-8.1.4.jar",
				"org.apache.jasper.glassfish-2.2.2.jar", 
				"javax.el-2.2.0.jar",
				"zookeeper-3.4.3.jar", 
				"log4j-1.2.15.jar",
				"slf4j-api-1.6.1.jar",
				"slf4j-log4j12-1.6.1.jar",
				"PaasZooClient.jar"};
		
		for (String jarName : jarNames) {
			this.resources.add(new Resource(jarName, String.format(HD_URI, this.hadoopHost, jarName)));
		}
	}
}
