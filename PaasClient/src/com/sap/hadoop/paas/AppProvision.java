package com.sap.hadoop.paas;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.UserInfo;
import com.jcraft.jsch.Session;

public class AppProvision {
	private String host;
	private String username;
	private String password;
	
	public AppProvision(String host, String username, String password) {
		this.host = host;
		this.username = username;
		this.password = password;
	}
	
	public boolean provision(String path) throws JSchException, IOException {
		JSch jsch = new JSch();
		Session session = jsch.getSession(username, host, 22);
		session.setConfig("StrictHostKeyChecking", "no");
		
		UserInfo userinfo = new MyUserInfo(password);
		session.setUserInfo(userinfo);
		session.connect();

		String fileName;
		if (path.lastIndexOf('/') > 0) {
			fileName = path.substring(path.lastIndexOf('/') + 1);
		} else {
			fileName = path;
		}
		
		if (!sendFile(session, path, fileName)) {
			return false;
		}
		
		this.execCommand(session, "hadoop fs -rm /PAAS/" + fileName);
		this.execCommand(session, String.format("hadoop fs -put ~/i827616/%s /PAAS/%s", fileName, fileName));
		
		session.disconnect();
		return true;
	}
	
	private boolean sendFile(Session session, String path, String fileName) throws JSchException, IOException {
		System.out.println("Copying the war file: " + path);
		
		String targetPath = "~/i827616/" + fileName;
		FileInputStream fis = null;
		String command = "scp -t " + targetPath;
		Channel channel = session.openChannel("exec");
		((ChannelExec) channel).setCommand(command);
		
		OutputStream out = channel.getOutputStream();
		InputStream in = channel.getInputStream();
		channel.connect();
		if (checkAck(in) != 0) {
			return false;
		}
		
		File source = new File(path);
		command = "C0644 " + source.length() + " ";
		command += fileName;
		command += "\n";
		out.write(command.getBytes());
		out.flush();
		if (checkAck(in) != 0) {
			return false;
		}
		
		fis = new FileInputStream(path);
		byte[] buf = new byte[1024];
		while(true) {
			int len = fis.read(buf, 0, buf.length);
			if (len <= 0) break;
			out.write(buf, 0, len);
		}
		fis.close();
		
		buf[0]=0; 
		out.write(buf, 0, 1); 
		out.flush();
		if (checkAck(in) != 0) {
			return false;
		}
		
		out.close();
		channel.disconnect();
		return true;		
	}
	
	private void execCommand(Session session, String command) throws JSchException, IOException {
		System.out.println("Executing a remote command: " + command);
		Channel channel = session.openChannel("exec");
		((ChannelExec) channel).setCommand(command);
		channel.setInputStream(null);
		((ChannelExec) channel).setErrStream(System.err);
		InputStream is = channel.getInputStream();
		channel.connect();
		
		byte[] tmp = new byte[1024];
		while (true) {
			while(is.available() > 0) {
				int count = is.read(tmp, 0, 1024);
				if (count < 0) break;
				System.out.println(new String(tmp, 0, count));
			}
			if (channel.isClosed()) {
				System.out.println("exit-status: " + channel.getExitStatus());
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (Exception ex) {			
			}
		}
		
		channel.disconnect();
	}

	private static int checkAck(InputStream in) throws IOException{
		int b=in.read();
		// b may be 0 for success,
		//          1 for error,
		//          2 for fatal error,
		//          -1
		if(b==0) return b;
		if(b==-1) return b;

		if(b==1 || b==2){
			StringBuffer sb=new StringBuffer();
			int c;
			do {
				c=in.read();
				sb.append((char)c);
			}
			while(c!='\n');
			if(b==1){ // error
				System.out.print(sb.toString());
			}
			if(b==2){ // fatal error
				System.out.print(sb.toString());
			}
		}
		return b;
	}

	private class MyUserInfo implements UserInfo {
		private String password;
		
		public MyUserInfo(String password) {
			this.password = password;
		}
		
		@Override
		public String getPassphrase() {
			return null;
		}

		@Override
		public String getPassword() {
			return password;
		}

		@Override
		public boolean promptPassphrase(String arg0) {
			return true;
		}

		@Override
		public boolean promptPassword(String arg0) {
			return true;
		}

		@Override
		public boolean promptYesNo(String arg0) {
			return false;
		}

		@Override
		public void showMessage(String msg) {
			System.out.println(msg);
		}	
	}
}
