package com.sap.hadoop.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import com.sap.zookeeper.ZooContainerClient;
import com.sap.zookeeper.api.ZooContainerInterface;

public class PaasAppContainer implements ZooContainerInterface {
	private Configuration conf = null;
	private FileSystem fs = null;
	private String serviceName;
	private String zookeeperAddress;
	private ZooContainerClient zkClient;
	private String warPath;
	private Server server;
	
	public PaasAppContainer(String serviceName, String hadoopHost, String zookeeperAddress) throws Exception {
		conf = new Configuration();
		this.serviceName = serviceName;
		String hdfsAddress = String.format("hdfs://%s:9000", hadoopHost);
		conf.set("fs.default.name", hdfsAddress);
		conf.set("yarn.resourcemanager.scheduler.address", hadoopHost + ":8030");
		fs = FileSystem.get(conf);
		
		this.zookeeperAddress = zookeeperAddress;
        server = new Server(0);
		String warPath = downloadWar(serviceName);
        WebAppContext webapp = new WebAppContext();
        webapp.setWar(warPath);
        webapp.setServer(server);
        server.setHandler(webapp);
        server.start();	    
	}

	@Override
	public void handleDeleted() {
		try {
			if (server != null) {
				server.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	public String downloadWar(String serviceName) throws Exception {
		warPath = String.format("/tmp/%s.%s.war", serviceName, Long.toString(System.currentTimeMillis()));		
		File file = new File(warPath);
		String hdPath = String.format("/PAAS/%s.war", serviceName);
		Path path = new Path(hdPath);
		if (fs.exists(path)) {
			getFileFromHadoop(path, warPath);
		} else {
			throw new Exception("Application is not provisioned");
		}
		
		return warPath;
	}
	
	public void register() throws Exception {
		int port = server.getConnectors()[0].getLocalPort();    		
        zkClient = new ZooContainerClient(this);
        zkClient.connect(this.zookeeperAddress);
        
        String hostPort = InetAddress.getLocalHost().getCanonicalHostName() + ":" + port;
        System.out.println("Port assigned: " + hostPort);
		        
        zkClient.register(serviceName, hostPort);		
	}
	
	public void join() throws InterruptedException {
		System.out.println("Server joining....");
		server.join();
	}
	
	public void unregister() {
		System.out.println("Unregsitering ...");
		if (zkClient != null) {
			zkClient.unregister();
		}
		
		if (warPath != null) {
			File file = new File(warPath);
			file.delete();
		}
		
		System.out.println("Unregistered successfully ...");
	}
	
	private void getFileFromHadoop(Path hadoopPath, String localPath) throws IOException {
		FileOutputStream fos = new FileOutputStream(localPath);
		FSDataInputStream his = fs.open(hadoopPath);
		byte[] buffer = new byte[2048];
		int count = 0;
		
		while ((count = his.read(buffer)) > 0) {
			fos.write(buffer, 0, count);			
		}
		
		fos.close();
		his.close();
		
		System.out.println("Downloaded: " + localPath);
	}	
	
	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: PaasAppContainer <SERVICE_NAME> <HADOOP_HOST> <ZOOKEEPER_HOST:PORT>");
			System.exit(1);
		}
		
		String serviceName = args[0];
		System.out.println(String.format("Started %s at %s", serviceName, new Date()));
    	
		PaasAppContainer container = null;
    	try {
    		container = new PaasAppContainer(serviceName, args[1], args[2]);
    		container.register();
    		container.join();
    	} catch (Exception ex) {
    		System.err.println("Error: " + ex.getMessage());
    		ex.printStackTrace();
    	} finally {
    		if (container != null) {
    			container.unregister();
    		}
    	}		
	}
}
