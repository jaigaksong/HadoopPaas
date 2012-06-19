package com.sap.zookeeper;

import java.util.List;

import org.apache.zookeeper.KeeperException;


public class PaasZooClient extends ConnectionWatcher {
	public List<String> getInstances(String serviceName) {
		String path = "/PAAS/" + serviceName;
		try {
			if (zk.exists(path, false) != null) {
				return zk.getChildren(path, false);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public void removeInstances(String serviceName, int numInstances) {
		List<String> instances = getInstances(serviceName);
		int deleted = 0;
		
		if (instances != null) {
			for (String instance : instances) {
				try {
					String path = String.format("/PAAS/%s/%s", serviceName, instance);
					zk.delete(path, -1);
					deleted++;
					System.out.println("Removed " + instance);
					if (deleted == numInstances) {
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (KeeperException e) {
					e.printStackTrace();
				}
			}
		}
		
		System.out.println(String.format("%d service instances removed", deleted));
	}
}
