package com.sap.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ChildrenWatcher extends ConnectionWatcher {
	private Map<String, ServiceNode> services = new HashMap<String, ServiceNode>();
	private String rootPath;
	
	@Override
	public void process(WatchedEvent event) {
		super.process(event);
		if (event.getType() == EventType.NodeChildrenChanged) {
			String path = event.getPath();
			System.out.println("NodeChildrenChanged event: " + path);
			if (rootPath.equals(path)) {
				try {
					List<String> children = zk.getChildren(path, true);
					this.setServiceNodes(children);
				} catch (KeeperException ex) {
					
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}
			} else {
				String[] items = path.split("/");
				ServiceNode node = services.get(items[2]);
				if (node != null) {
					node.updateChildren();
				}
			}
		}
	}
	
	public void connect(String hosts, String rootPath) throws IOException, InterruptedException {
		this.rootPath = rootPath;
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();

		try {
			if (zk.exists(rootPath, false) == null) {
				zk.create(rootPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException ex) {
			//ignore
		}
		
		try {
			List<String> children = zk.getChildren(rootPath, true);
			this.setServiceNodes(children);
		} catch (KeeperException ex) {
			
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}		
	}
	
	public String getNextInstance(String service) {
		ServiceNode node = services.get(service);
		return node.getNextInstance();
	}
	
	private synchronized void setServiceNodes(List<String> nodes) {
		for (String service : nodes) {
			ServiceNode node = services.get(service);
			if (node == null) {
				String path = rootPath + "/" + service;
				node = new ServiceNode(service, path, zk);
				services.put(service, node);
			}
		}		
	}
}
