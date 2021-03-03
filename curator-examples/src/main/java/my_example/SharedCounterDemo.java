package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class SharedCounterDemo {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();

        SharedCount sharedCount = new SharedCount(
                client, "/shared/count", 0);
        sharedCount.start();

        sharedCount.addListener(new SharedCountListener() {

            public void countHasChanged(SharedCountReader sharedCountReader, int i) throws Exception {
                System.out.println("分布式计数器变化了......");
            }

            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                System.out.println("连接状态变化了.....");
            }

        });

        Boolean result = sharedCount.trySetCount(1);

        System.out.println(sharedCount.getCount());
    }

}
