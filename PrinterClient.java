import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class PrinterClient implements Runnable {
	private static BufferedReader in ;  static int quit=0;
	private static final int SESSION_TIMEOUT = 5000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher()
				{
				public void process(WatchedEvent event) { // Watcher interface
			if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
			}}});

		connectedSignal.await();
	}
	
	public void create(String groupName,String data) throws KeeperException,
	InterruptedException {
		try
		{
			String path = groupName;
			String createdPath = zk.create(path,data.getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("Created " + createdPath);
			zk.exists(createdPath, new Watcher(){
				public void process(WatchedEvent event) { // Watcher interface

			if(event.getType()==EventType.NodeDeleted)
			{
				System.out.println("Node deleted");
			}
			if(event.getType()==EventType.NodeCreated)
			{
				System.out.println("Your print job is processed.Check the printer");
			}
				}});
		}
		catch(Exception e)
		{
			System.out.println("Znode representing Printer Queue has not been created by the Master");
			zk.close();
		}
			}
	public void close() throws InterruptedException {
		zk.close();
	}
	public void run(){
		String msg = null;
		while(true){
			try{
				msg=in.readLine();
			}catch(Exception e){}

			if(msg.equals("quit")) {quit=1;break;}
		}
	}
		public static void main(String[] args) throws Exception {
			in=new BufferedReader(new InputStreamReader(System.in));

			Thread t1=new Thread(new PrinterClient());
			t1.start();

			System.out.println("press quit then ENTER to terminate");

			while(true){
				PrinterClient pc = new PrinterClient();	
				pc.connect("localhost:2181");
				pc.create("/vmudunu/"+args[0],args[1]);	
				
				Thread.sleep(10);
				if(quit==1) 
					pc.close();
					break;

			}
		}
}
