package com.sap.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ConnectionWatcher implements Watcher, Runnable{
	protected static final int SESSION_TIMEOUT = 5000;
	protected ZooKeeper zk;
	protected CountDownLatch connectedSignal = new CountDownLatch(1);
	protected String zkHosts;
	protected boolean connected = false;
	protected boolean closed = false;
	
	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == EventType.None) {
			if(event.getState() == KeeperState.SyncConnected) {		
				System.out.println("Connected to ZooKeeper...");
				connectedSignal.countDown();						
			} else if (event.getState() == KeeperState.Disconnected) {
				if (!closed) {
					connected =false;
					Thread thread = new Thread(this);
					thread.start();
				}
			}
		} 
	}
	
	public void connect(String hosts) throws IOException, InterruptedException {
		this.zkHosts = hosts;
		run();
	}

	public void close() throws InterruptedException {
		try {
			zk.close();
		} finally {
			closed = true;
		}
	}

	@Override
	public void run() {
		while(!closed && !connected) {
			try{
				connectZK();
			} catch (Exception ex) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					// ignore;
				}
			}
		}
	}	
	
	private synchronized void connectZK() throws IOException, InterruptedException {
		connectedSignal = new CountDownLatch(1);
		zk = new ZooKeeper(zkHosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
		connected = true;		
	}
}
