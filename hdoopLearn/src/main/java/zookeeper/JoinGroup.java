package zookeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class JoinGroup extends ConnectionWatcher {

	public void joinGroup(String groupName, String memberName) throws KeeperException, InterruptedException {
		String path = "/" + groupName + "/" + memberName;
		create(path, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		JoinGroup joinGroup = new JoinGroup();
		joinGroup.connect("192.168.245.128");// zk的ip，在不同的机器，请改为不同ip
		joinGroup.joinGroup("myFirstGroup", "child1G");
		TimeUnit.SECONDS.sleep(10);
	}

}
