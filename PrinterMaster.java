import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class PrinterMaster implements Runnable {
private static BufferedReader in ;  static int quit=0;
private static final int SESSION_TIMEOUT = 500000;
private ZooKeeper zk;
private CountDownLatch connectedSignal = new CountDownLatch(1);
public void connect(String hosts) throws IOException, InterruptedException {
zk = new ZooKeeper(hosts, SESSION_TIMEOUT,new Watcher(){
public void process(WatchedEvent event) { // Watcher interface
if (event.getState() == KeeperState.SyncConnected) {
connectedSignal.countDown();
}}});
connectedSignal.await();
}
public void create(String groupName) throws KeeperException,
InterruptedException {
String path = groupName;
String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
CreateMode.PERSISTENT);	
System.out.println("Created " + createdPath);
zk.exists(createdPath, new Watcher(){
	public void process(WatchedEvent event) { // Watcher interface
		if(event.getType()==EventType.NodeDeleted)
		{
			System.out.println("Node deleted");
		}
		if(event.getType()==EventType.NodeCreated)
		{
			System.out.println("Print job with the given id has been processed");
		}
		}});
}
public void delete(String groupName) throws KeeperException,
InterruptedException {
String path = groupName;
try {
List<String> children = zk.getChildren(path, false);
for (String child : children) {
zk.delete(path + "/" + child, -1);
}
zk.delete(path, -1);
} catch (KeeperException.NoNodeException e) {
System.out.printf("Znode %s does not exist\n", groupName);
System.exit(1);
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

Thread t1=new Thread(new PrinterMaster());
t1.start();

System.out.println("press quit then ENTER to terminate");

while(true){
	PrinterMaster pm = new PrinterMaster();
	pm.connect("localhost:2181");
	pm.create("/vmudunu/PrinterQueue");

	Thread.sleep(10);
	if(quit==1)
		pm.delete("/vmudunu/PrinterQueue");
	    pm.close();

		break;

}
}
}
