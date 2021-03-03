package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;

public class ChildrenWatcher {
    public static void main(String[] args) throws Exception{
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();

        System.out.println("已经启动Curator客户端");

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/test", "100".getBytes());

        client.getChildren().usingWatcher(new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                System.out.println("event:" + event);
            }
        }).inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("event:" + event);
            }
        }).forPath("/test");
    }
}
