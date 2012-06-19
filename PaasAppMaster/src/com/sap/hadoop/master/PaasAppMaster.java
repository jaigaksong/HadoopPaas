package com.sap.hadoop.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class PaasAppMaster {
	private int numTotalContainers = 1;
	private Configuration conf = null;
	private YarnRPC rpc = null;
	private FileSystem fs = null;
	private ApplicationAttemptId appAttemptID = null;
	private AMRMProtocol resourceManager = null;
	private AtomicInteger rmRequestId = new AtomicInteger();
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numRequestedContainers = new AtomicInteger();
	private boolean appDone = false;
	private List<Thread> launchThreads = new ArrayList<Thread>();
    private CopyOnWriteArrayList<ContainerId> releasedContainers = new CopyOnWriteArrayList<ContainerId>();
    private MasterConfig masterConfig;
	
	public PaasAppMaster(String serviceName, String numTotalContainers, String memSize, String hadoopHost, String zkHostPort) throws Exception {
		conf = new Configuration();
		conf.set("fs.default.name", String.format("hdfs://%s:9000", hadoopHost));
		conf.set("yarn.resourcemanager.scheduler.address", hadoopHost + ":8030");
		rpc = YarnRPC.create(conf);
		fs = FileSystem.get(conf);
		
		masterConfig = new MasterConfig(serviceName, Integer.parseInt(memSize), hadoopHost, zkHostPort);
		this.numTotalContainers = Integer.parseInt(numTotalContainers);
	}
	
	public boolean run() throws YarnRemoteException {
		resourceManager = connectToRM();
		RegisterApplicationMasterResponse response = registerToRM();

		while (numCompletedContainers.get() < numTotalContainers && !appDone) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			int askCount = numTotalContainers - numRequestedContainers.get();
			numRequestedContainers.addAndGet(askCount);
			
			List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
			if (askCount > 0) {
				System.out.println("Container asking count: " + askCount);
				ResourceRequest containerAsk = this.setupContainerRequest(askCount);
				resourceReq.add(containerAsk);
			}
			
			AMResponse amResp = this.sendContainerRequestToRM(resourceReq);
			List<Container> allocatedContainers = amResp.getAllocatedContainers();
			numAllocatedContainers.addAndGet(allocatedContainers.size());
			for (Container allocatedContainer : allocatedContainers)  {
				LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, masterConfig);
				Thread launchThread = new Thread(runnableLaunchContainer);
				launchThreads.add(launchThread);
				launchThread.start();
			}
			
			List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
			for (ContainerStatus containerStatus : completedContainers) {
				int exitStatus = containerStatus.getExitStatus();
				if (exitStatus != 0) {
					if (exitStatus != 100) {
						numCompletedContainers.incrementAndGet();
						numFailedContainers.incrementAndGet();
					} else {
						numAllocatedContainers.decrementAndGet();
						numRequestedContainers.decrementAndGet();
					}
				} else {
					numCompletedContainers.incrementAndGet();
				}
			}
			
			if (numCompletedContainers.get() == numTotalContainers) {
				appDone = true;
			}
		}
		
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
		finishReq.setAppAttemptId(appAttemptID);
		boolean isSuccess = true;
		if (numFailedContainers.get() == 0) {
			finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
			System.out.println("Finished successfully.");
		}
		else {
			finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
			String diagnostics = "Diagnostics."
					+ ", total=" + numTotalContainers
					+ ", completed=" + numCompletedContainers.get()
					+ ", allocated=" + numAllocatedContainers.get()
					+ ", failed=" + numFailedContainers.get();
			
			System.out.println(diagnostics);
			finishReq.setDiagnostics(diagnostics);
			isSuccess = false;
		}
		
		resourceManager.finishApplicationMaster(finishReq);
		return isSuccess;
	}
	
	private AMRMProtocol connectToRM() {
	    Map<String, String> envs = System.getenv();
	    String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
	    if (containerIdString == null) {
	      // container id should always be set in the env by the framework 
	      throw new IllegalArgumentException("ContainerId not set in the environment");
	    }
	    
	    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
	    appAttemptID = containerId.getApplicationAttemptId();
	    
	    // Connect to the Scheduler of the ResourceManager. 
	    YarnConfiguration yarnConf = new YarnConfiguration(conf);
	    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
	            YarnConfiguration.RM_SCHEDULER_ADDRESS,
	            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));    
	                   
	    System.out.println("AppMaster connecting to ResourceManager at " + rmAddress);
	    return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);
	}
	
	private RegisterApplicationMasterResponse registerToRM() throws YarnRemoteException {
	    RegisterApplicationMasterRequest appMasterRequest = 
		        Records.newRecord(RegisterApplicationMasterRequest.class);
	    appMasterRequest.setApplicationAttemptId(appAttemptID);    
	    java.net.InetAddress addr = null;
	    try {
	    	addr = java.net.InetAddress.getLocalHost();
	    } catch (UnknownHostException e) {
	    	e.printStackTrace();
	    }

	    appMasterRequest.setHost(addr.getHostAddress());
	    appMasterRequest.setRpcPort(8099);

	    return resourceManager.registerApplicationMaster(appMasterRequest);
	}
	
	private ResourceRequest setupContainerRequest(int numContainers) {
	    ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);
	    rsrcRequest.setHostName("*");

	    // set the priority for the request
	    Priority pri = Records.newRecord(Priority.class);
	    pri.setPriority(0);
	    rsrcRequest.setPriority(pri);           

	    // Set up resource type requirements
	    // For now, only memory is supported so we set memory requirements
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory(masterConfig.getMemory());
	    rsrcRequest.setCapability(capability);		
	    rsrcRequest.setNumContainers(numContainers);	    
	    
	    return rsrcRequest;
	}
	
	private AMResponse sendContainerRequestToRM(List<ResourceRequest> requestedContainers) throws YarnRemoteException {
	    AllocateRequest req = Records.newRecord(AllocateRequest.class);

	    req.setResponseId(rmRequestId.incrementAndGet());
	    req.setApplicationAttemptId(appAttemptID);
	    req.addAllAsks(requestedContainers);
	    req.addAllReleases(releasedContainers);
	    req.setProgress(numCompletedContainers.get()/numTotalContainers);
	    
	    if (requestedContainers.size() > 0) {
	    	System.out.println(String.format("Sending allocation request: requestedSet=%d, releasedSet=%d", 
	    			requestedContainers.size(), releasedContainers.size()));
	    }
	    
	    AllocateResponse allocateResponse = resourceManager.allocate(req); 		
		return allocateResponse.getAMResponse();
	}
	
	public static void main(String[] args) {
		boolean result = false;
		if (args.length != 5) {
			System.out.println("Usage: PassAppMaster <SERVICE_NAME> <NUM_INSTANCES> <MEMORY_SIZE> <HADOOP_HOST> <ZOOKEEPER_HOST:PORT>");
			System.exit(1);
		}
		
		try {
			PaasAppMaster master = new PaasAppMaster(args[0], args[1], args[2], args[3], args[4]);
			result = master.run();
		} catch (Exception ex) {
			System.exit(1);
		}
		
		if (result) {
			System.exit(0);
		} else {
			System.exit(2);
		}
	}
	
	private class LaunchContainerRunnable implements Runnable {
		Container container;
		ContainerManager cm;
		MasterConfig masterConfig;
		
		public LaunchContainerRunnable (Container container, MasterConfig masterConfig) {
			this.container = container;
			this.masterConfig = masterConfig;
		}
		
		@Override
		public void run() {
			System.out.println(String.format("Launching a container: nodeId=%s, containerId=%s ", container.getNodeId(), container.getId()));
			connectToConnectionManager();
			ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
			context.setContainerId(container.getId());
			context.setResource(container.getResource());
			
			try {
				context.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		    Map<String, String> env = new HashMap<String, String>();
	        String classPathEnv = System.getProperty("java.class.path") + ":./*:";	        
	        System.out.println("CLASSPATH: " + classPathEnv);
	        env.put("CLASSPATH", classPathEnv);
		    context.setEnvironment(env);
		    
		    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		    
		    try {
		    	for (MasterConfig.Resource resource : masterConfig.getResources()) {
		    		this.addResource(localResources, resource.getPath(), resource.getName());
		    	}
		    } catch (IOException e) {
		    	e.printStackTrace();
		    }
		    context.setLocalResources(localResources);         
		    String command = masterConfig.getCommand() 
		    		+ " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
		    		+ " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

		    List<String> commands = new ArrayList<String>();
		    commands.add(command);
		    context.setCommands(commands);

		    // Send the start request to the ContainerManager
		    StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
		    startReq.setContainerLaunchContext(context);
		    System.out.println("Staring container: " + container.getId());
		    try {
		    	cm.startContainer(startReq);	
		    } catch (YarnRemoteException e) {
		    	System.out.println("Error: " + e.getMessage());
		    	e.printStackTrace();
		    }
		}
		
		private void connectToConnectionManager() {
			String cmIpPortStr = container.getNodeId().getHost() + ":"  + container.getNodeId().getPort();
			InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
			this.cm = (ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, conf);
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
	}
}
