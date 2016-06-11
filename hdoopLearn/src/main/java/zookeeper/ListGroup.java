package zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;

public class ListGroup extends ConnectionWatcher {
	
	public void listGroup(String groupName) throws KeeperException, InterruptedException {
		groupName = "/" + groupName;
		List<String> childrenNames = zk.getChildren(groupName, false);// 等下测测这个watch的作用
		System.out.println("--" + groupName);
		for (String name : childrenNames) {
			System.out.println("\t --" + name);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		ListGroup listGroup = new ListGroup();
		listGroup.connect("192.168.245.128");// zk的ip，在不同的机器，请改为不同ip
		listGroup.listGroup("myFirstGroup");
	}

}
