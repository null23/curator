package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ChildrenWatcherDemo {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();
    }
}
