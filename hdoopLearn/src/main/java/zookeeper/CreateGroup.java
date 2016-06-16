package zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class CreateGroup implements Watcher {
	
	private ZooKeeper zk;
	
	private int timeout = 1000;
	
	private CountDownLatch countDownLatch = new CountDownLatch(1);
	
	public void connect(String hostAndPort) throws IOException, InterruptedException {
		zk = new ZooKeeper(hostAndPort, timeout, this);
		try {
			countDownLatch.await(2, TimeUnit.SECONDS);// zk创建连接是异步的，所以等到回调才成功才能返回。
		} catch (Exception e) {
			System.out.println("countDownLatch timeout !!!");
			throw e;
		}
	}

	@Override
	public void process(WatchedEvent arg0) {
		System.out.println("createGroup processing ...........");
		if (arg0.getState() == KeeperState.SyncConnected ) {
			countDownLatch.countDown();
		}
	}
	
	public void create(String gropuName) throws KeeperException, InterruptedException {
		gropuName = "/" + gropuName;
		String createPath = zk.create(gropuName, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("created znode: " + createPath);
	}
	
	public void close() throws InterruptedException {
		zk.close();
	}
	
	public static void main(String[] args) throws Exception {
		CreateGroup group = new CreateGroup();
		group.connect("192.168.245.128");// zk的ip，在不同的机器，请改为不同ip
		group.create("myFirstGroup");
		group.close();
	}

}
