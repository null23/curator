package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;

public class CrudDemo {

    public static void main(String[] args) throws Exception {
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
                .forPath("/my/path", "100".getBytes());
        byte[] dataBytes = client.getData().forPath("/my/path");
        System.out.println(new String(dataBytes));

        client.setData().forPath("/my/path", "110".getBytes());
        dataBytes = client.getData().forPath("/my/path");
        System.out.println(new String(dataBytes));

        List<String> children = client.getChildren().forPath("/my");
        System.out.println(children);

        client.delete().forPath("/my/path");

        Thread.sleep(Integer.MAX_VALUE);
    }

}
