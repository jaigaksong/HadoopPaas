package com.sap.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.ZooKeeper;

public class ServiceNode {
	private String name;
	private String path;
	private ZooKeeper zk;
	private List<String> instances = new ArrayList<String>();
	private int index = 0;
	
	public ServiceNode(String name, String path, ZooKeeper zk) {
		this.name = name;
		this.path = path;
		this.zk = zk;
		System.out.println("Service node created: " + name);
		
		updateChildren();
	}
	
	public void updateChildren() {
		try {
			List<String> children = zk.getChildren(path, true);
			setInstances(children);
		} catch (Exception ex) {
			ex.printStackTrace();
		}		
	}
	
	public String getNextInstance() {
		List<String> currentInstances = instances;
		if (currentInstances.size() == 0) {
			return null;
		}
		
		index = index % currentInstances.size();
		return currentInstances.get(index++);
	}
	
	private synchronized void setInstances(List<String> instances) {
		if (instances != null && instances.size() > 0) {
			index = index % instances.size();
		}
		
		this.instances = instances;
		System.out.println(name + ": " + instances);
	}	
}
