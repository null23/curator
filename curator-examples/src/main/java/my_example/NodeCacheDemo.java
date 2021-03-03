package my_example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

public class NodeCacheDemo {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                1000, 3);
        final CuratorFramework client = CuratorFrameworkFactory.newClient(
                "localhost:2181",
                5000,
                3000,
                retryPolicy);
        client.start();

        final NodeCache nodeCache = new NodeCache(client, "/cluster");
        nodeCache.start();

        nodeCache.getListenable().addListener(new NodeCacheListener() {

            public void nodeChanged() throws Exception {
                Stat stat = client.checkExists().forPath("/cluster");
                if(stat == null) {

                } else {
                    nodeCache.getCurrentData();
                }
            }

        });
    }

}
