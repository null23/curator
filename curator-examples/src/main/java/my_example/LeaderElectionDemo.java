package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class LeaderElectionDemo {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();

        LeaderSelector leaderSelector = new LeaderSelector(
                client,
                "/leader/election",
                new LeaderSelectorListener() {

                    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                        System.out.println("你已经成为了leader......");

                        // 在这里干leader所有的事情，此时方法不能退出

                        Thread.sleep(Integer.MAX_VALUE);
                    }

                    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                        System.out.println("连接状态的变化......");
                        if(connectionState.equals(ConnectionState.LOST)) {
                            throw new CancelLeadershipException();
                        }
                    }

                });

        leaderSelector.start();

        Thread.sleep(Integer.MAX_VALUE);
    }

}
