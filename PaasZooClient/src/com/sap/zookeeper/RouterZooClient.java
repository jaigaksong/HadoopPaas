package com.sap.zookeeper;

public class RouterZooClient extends ChildrenWatcher {
	//private static final String zkHostPort = "palm00498842a.dhcp.pal.sap.corp:2181";
	private static final String serviceRoot = "/PAAS";
	public RouterZooClient(String zkHostPort) throws Exception {
		this.connect(zkHostPort, serviceRoot);
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Usage: RouterZooClient <ZooKeeperHost:Port>");
			System.exit(1);
		}
		
		RouterZooClient client = new RouterZooClient(args[0]);		
		Thread.sleep(Long.MAX_VALUE);
	}
}
