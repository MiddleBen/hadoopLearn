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

public class ConnectionWatcher implements Watcher {

	protected ZooKeeper zk;

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
		System.out.println("createGroup processing ..........., state = " + arg0.getState());
		if (arg0.getState() == KeeperState.SyncConnected) {
			countDownLatch.countDown();
		}
	}

	public void create(String groupName, CreateMode createMode) throws KeeperException, InterruptedException {
		String createPath = zk.create(groupName, null, Ids.OPEN_ACL_UNSAFE, createMode);
		System.out.println("created znode: " + createPath);
	}

	public void close() throws InterruptedException {
		zk.close();
	}
}
