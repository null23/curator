package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class PathCacheDemo {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();

        PathChildrenCache pathChildrenCache = new PathChildrenCache(
                client, "/cluster", true);
        pathChildrenCache.start();

        // cache就是把zk里的数据缓存到了你的客户端里来
        // 你可以针对这个缓存的数据加监听器，去观察zk里的数据的变化

        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {

            }

        });
    }

}
