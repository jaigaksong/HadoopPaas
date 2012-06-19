package com.sap.hadoop.paas;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.jcraft.jsch.JSchException;
import com.sap.zookeeper.PaasZooClient;

public class PaasClient {
	private static String hadoopClassPath;
	private String hadoopHost;
	private String zkHostPort;
	private Configuration conf;
	private YarnRPC rpc = null;	
	private ClientRMProtocol applicationsManager;
	private FileSystem fs;
	private List<ApplicationReport> latestAppReports;
	
	static {
		StringBuffer classPathEnv = new StringBuffer("/usr/local/hadoop/share/hadoop/common/hadoop-common-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/mapreduce/hadoop-yarn-api-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/mapreduce/hadoop-yarn-common-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-app-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-common-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/hadoop-auth-0.23.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/log4j-1.2.15.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/commons-collections-3.2.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/commons-configuration-1.6.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/commons-lang-2.5.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/commons-logging-1.1.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/commons-logging-api-1.1.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/guava-r09.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/log4j-1.2.15.jar:");
		classPathEnv.append("/usr/local/hadoop/share/hadoop/common/lib/protobuf-java-2.4.0a.jar:./*:");  
		hadoopClassPath = classPathEnv.toString();
	}
	
	public PaasClient(String hadoopHost, String zkHostPort) throws IOException {
		conf = new Configuration();
		conf.set("fs.default.name", String.format("hdfs://%s:9000", hadoopHost));
		conf.set("yarn.resourcemanager.address", hadoopHost + ":8040");		

	    rpc = YarnRPC.create(conf);
		fs = FileSystem.get(conf);

		this.hadoopHost = hadoopHost;
		this.zkHostPort = zkHostPort;		
		connectToResourceManager();
	}

	private void connectToResourceManager() {
	    YarnConfiguration yarnConf = new YarnConfiguration(conf);
	    
	    InetSocketAddress rmAddress =
	        NetUtils.createSocketAddr(yarnConf.get(
	            YarnConfiguration.RM_ADDRESS,
	            YarnConfiguration.DEFAULT_RM_ADDRESS));             
	   
	    System.out.println("Connecting to ResourceManager at " + rmAddress);
	    applicationsManager = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, conf));    	
	}
	
	public void printAllApplications() throws YarnRemoteException {
		GetAllApplicationsRequest request = Records.newRecord(GetAllApplicationsRequest.class);
		GetAllApplicationsResponse response = applicationsManager.getAllApplications(request);
		List<ApplicationReport> reports = response.getApplicationList();
		latestAppReports = reports;
		for (ApplicationReport report : reports) {
			YarnApplicationState state = report.getYarnApplicationState();
			if (state == YarnApplicationState.NEW || state == YarnApplicationState.RUNNING || state == YarnApplicationState.SUBMITTED) {
				System.out.println(report.toString());
			}
		}
	}
	
	public void startInstances(String serviceName, int memSize, int numContainers) throws IOException {
		System.out.println(String.format("Starting %d instances of %s", numContainers, serviceName));

		GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);              
        GetNewApplicationResponse response = applicationsManager.getNewApplication(request);
        ApplicationId appId = response.getApplicationId();
        System.out.println("Got a new ApplicationId=" + appId.getId());		
        
        // Create a new ApplicationSubmissionContext
        ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
        appContext.setApplicationId(appId);
        appContext.setApplicationName(serviceName);
        
        // Create a new container launch context for the AM's container
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // Define the local resources required 
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        String masterJarPath = String.format("hdfs://%s:9000/PAAS/lib/PaasAppMaster.jar", hadoopHost);
        this.addResource(localResources, masterJarPath, "PaasAppMaster.jar");   
        amContainer.setLocalResources(localResources);

        // Set up the environment needed for the launch context
        Map<String, String> env = new HashMap<String, String>();    
        env.put("CLASSPATH", hadoopClassPath);
        amContainer.setEnvironment(env);
        
        // Construct the command to be executed on the launched container 
        String command = "${JAVA_HOME}/bin/java com.sap.hadoop.master.PaasAppMaster "
                + serviceName + " " + numContainers + " "
                + memSize + " "
                + hadoopHost + " " + zkHostPort 
    	        + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
    	        + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

        List<String> commands = new ArrayList<String>();
        commands.add(command);
        // add additional commands if needed                

        // Set the command array into the container spec
        amContainer.setCommands(commands);
        
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1024);
        amContainer.setResource(capability);
        
        // Set the container launch content into the ApplicationSubmissionContext
        appContext.setAMContainerSpec(amContainer);

        // Create the request to send to the ApplicationsManager 
        SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
        appRequest.setApplicationSubmissionContext(appContext);

        // Submit the application to the ApplicationsManager
        // Ignore the response as either a valid response object is returned on 
        // success or an exception thrown to denote the failure
        applicationsManager.submitApplication(appRequest);      
        
        System.out.println("Request has been submitted...");
	}
	
	public void killApplication(int appId) {
		ApplicationId applicationId;
		try {
			applicationId = this.getApplicationId(appId);
			if (applicationId != null) {
				this.killApplication(applicationId);
			}
		} catch (YarnRemoteException e) {
			e.printStackTrace();
		}
	}
	
	private void killApplication(ApplicationId appId) {
		KillApplicationRequest killRequest = Records.newRecord(KillApplicationRequest.class);                
	    killRequest.setApplicationId(appId);
	    try {
			applicationsManager.forceKillApplication(killRequest);
		} catch (YarnRemoteException e) {
			System.out.println("Failed to kill the application: " + appId);
			e.printStackTrace();
		}      				
	}
	
	private ApplicationId getApplicationId(int appId) throws YarnRemoteException {
		if (latestAppReports != null) {
			List<ApplicationReport> reports = latestAppReports;
			for (ApplicationReport report : reports) {
				if (report.getApplicationId().getId() == appId) {
					return report.getApplicationId();
				}				
			}
		}
		
		GetAllApplicationsRequest request = Records.newRecord(GetAllApplicationsRequest.class);
		GetAllApplicationsResponse response = applicationsManager.getAllApplications(request);
		List<ApplicationReport> reports = response.getApplicationList();
		latestAppReports = reports;
		for (ApplicationReport report : reports) {
			if (report.getApplicationId().getId() == appId) {
				return report.getApplicationId();
			}
		}
		
		return null;
	}
	
	public void printNodes() throws YarnRemoteException {		
		GetClusterNodesRequest request = Records.newRecord(GetClusterNodesRequest.class);
		try {
			GetClusterNodesResponse response = applicationsManager.getClusterNodes(request);
			List<NodeReport> reports = response.getNodeReports();
			for (NodeReport report : reports) {
				System.out.println(report.toString());
			}
		} catch (YarnRemoteException ex) {
			System.out.println(ex.getMessage());
		}
	}
	
	private void addResource(Map<String, LocalResource> localResources, String url, String name) throws IOException {
	    Path jarPath = new Path(url);
        FileStatus jarStatus = fs.getFileStatus(jarPath);
        System.out.println(name + " jarStatus: " + jarStatus.getLen());

        LocalResource shellRsrc = Records.newRecord(LocalResource.class);
	    shellRsrc.setType(LocalResourceType.FILE);
	    shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);          
	    shellRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
	    shellRsrc.setTimestamp(jarStatus.getModificationTime());
	    shellRsrc.setSize(jarStatus.getLen());
	    localResources.put(name, shellRsrc);		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length != 4) {
			System.err.println("Usage: PaasClient <ZOOKEEPER_HOST:PORT> <HADOOP_HOST> <HADOOP_USERNAMR> <HADOOP_PASSWORD>");
			System.exit(1);
		}
		
		AppProvision appProvision = new AppProvision(args[1], args[2], args[3]);
		PaasClient client = new PaasClient(args[1], args[0]);
		PaasZooClient zkClient = new PaasZooClient();
		zkClient.connect(args[0]);		
		
		System.out.print(">");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));		
		while (true) {
			String line = br.readLine();
			String[] items = line.split(" ");
			if (items.length == 0) {
				System.out.print(">");
				continue;
			}
			
			if ("list".equals(items[0]) || "l".equals(items[0])) {
				client.printAllApplications();
			} else if ("quit".equals(items[0]) || "q".equals(items[0])) {
				zkClient.close();
			    System.out.println("PaasClient is stopped.");
			    System.exit(0);
			} else if ("kill".equals(items[0])) {
				if (items.length != 2) {
					System.out.println("Usage: kill <APPLICATON_ID>");
				} else {
					client.killApplication(Integer.parseInt(items[1]));
				}
			} else if ("start".equals(items[0])) {
				if (items.length != 4) {
					System.out.println("Usage: start <SERVICE_NAME> <MEMORY_SIZE> <NUM_INSTANCES>");
				} else {
					client.startInstances(items[1], Integer.parseInt(items[2]), Integer.parseInt(items[3]));
				}
			} else if ("stop".equals(items[0])) {
				if (items.length != 3) {
					System.out.println("Usage: stop <SERVICE_NAME> <NUM_INSTANCES>");
				} else {
					zkClient.removeInstances(items[1], Integer.parseInt(items[2]));
				}
			} else if ("instances".equals(items[0]) || "i".equals(items[0])) {    
			    if (items.length == 1) {
			    	System.out.println("Usage: instances <SERVICE_NAME>");
			    } else {
			    	List<String> instances = zkClient.getInstances(items[1]);
			    	if (instances != null) {
			    		System.out.println(String.format("%d instances in total:", instances.size()));
			    		for (String instance : instances) {
			    			System.out.println("   " + instance);
			    		}
			    	}
			    }
			} else if ("push".equals(items[0])) {
				if (items.length != 2) {
					System.out.println("Usage: push <WAR_FILE_PATH>");
				} else {
					File warFile = new File(items[1]);
					if (!items[1].endsWith(".war")) {
						System.out.println("Invalid file extension. Only war files are supported");
					} else if (warFile.exists()) {
						try {
							if (!appProvision.provision(items[1])) {
								System.out.println("Failed to provision");
							}
						} catch (JSchException e) {
							System.out.println("Failed to provision: " + e.getMessage());
							e.printStackTrace();
						}

					} else {
						System.out.println("File does not exist");
					}
				}
			} else if ("nodes".equals(items[0])) {
				client.printNodes();
			} else if ("help".equals(items[0]) || "h".equals(items[0])) {
				printHelps();
			} else {
				System.out.println("Invalid command: " + items[0]);
			}
			
			System.out.print(">");
		}		
	}
	
	private static void printHelps() {
		System.out.println("help       ==> lists all available commands");
		System.out.println("list       ==> lists all yarn applications");
		System.out.println("push <WAR_FILE_PATH>         ==> provisions a service");
		System.out.println("start <SERVICE_NAME> <MEMORY_SIZE> <NUM_INSTANCES>        ==> starts service instances");
		System.out.println("stop <SERVICE_NAME> <NUM_INSTANCES>         ==> stops service instances");
		System.out.println("instances <SERVICE_NAME>     ==> lists all the available service instances");
		System.out.println("kill <YARN_APPLICATION_ID>   ==> kills forcefully an yarn application");
		System.out.println("quit       ==> exits the client");
	}
}
