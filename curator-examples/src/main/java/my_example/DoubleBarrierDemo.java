package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class DoubleBarrierDemo {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();

        DistributedDoubleBarrier doubleBarrier = new DistributedDoubleBarrier(
                client, "/barrier/double", 10);
        doubleBarrier.enter(); // 每台机器都会阻塞在enter这里
        // 直到10台机器都调用了enter，就会从enter这里往下走

        // 可以做一些计算任务

        doubleBarrier.leave(); // 每台机器都会阻塞在leave这里，直到10台机器都调用了leave
        // 此时就可以继续往下走
    }

}
