package com.sap.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

import com.sap.zookeeper.api.ZooContainerInterface;

public class ZooContainerClient extends ConnectionWatcher {
	private ZooContainerInterface container;
	private String service;
	private String hostPort;
	
	public ZooContainerClient(ZooContainerInterface container) {
		this.container = container;
	}
	
	public void register(String service, String hostPort) throws Exception {
		try {
			createGroup(service);
			joinGroup(service, hostPort);
			this.service = service;
			this.hostPort = hostPort;
		} catch (Exception ex) {
			throw new Exception("Failed to register", ex);
		}		
	}
	
	public void unregister() {
		try {
			this.close();
			System.out.println("Unregistered successfully");
		} catch (InterruptedException ex) {
			
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		super.process(event);
		if (event.getType() == EventType.NodeDeleted) {
			String path = event.getPath();
			System.out.println("NodeDeleted event: " + path);
			container.handleDeleted();
		}
	}
	
	public void run() {
		super.run();
		if (this.service != null && this.hostPort != null && !closed) {
			try {
				joinGroup(service, hostPort);
			} catch (Exception ex) {
				System.out.println("Failed to register: " + ex.getMessage());
			}
		}
	}
	
	private void createGroup(String groupName) throws KeeperException, InterruptedException {
		String path = "/PAAS/" + groupName;
		if (zk.exists(path, false) == null) {
			String createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("Created " + createdPath);
		} else {
			System.out.println("Group already exists: " + path);
		}		
	}
	
	private void joinGroup(String groupName, String memberName) throws KeeperException, InterruptedException {
		String path = String.format("/PAAS/%s/%s", groupName, memberName);
		if (zk.exists(path, true) == null) {
			String createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zk.exists(path, true);
			System.out.println("Created " + createdPath);
		} else {
			System.out.println("Member already exists: " + path);
		}
	}
}
