package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class LeaderLatchDemo {

    public static void main(String[] args) throws Exception {
       RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                        1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();

        LeaderLatch leaderLatch = new LeaderLatch(client, "/leader/latch");
        leaderLatch.start();
        leaderLatch.await(); // 直到等待他成为leader再往后走

        // 类似于HDFS里，两台机器，其中一台成为了leader就往后走，开始工作
        // 另外一台机器可以通过await阻塞在这里，直到leader死了，自己就会成为leader

        Boolean hasLeaderShip = leaderLatch.hasLeadership();

        System.out.println("是否成为leader：" + hasLeaderShip);

        Thread.sleep(Integer.MAX_VALUE);
    }

}
