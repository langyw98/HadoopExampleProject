package com.kgc.hadoop;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by kgc on 2017/11/20.
 */
public class ZKApp {
    private ZooKeeper zooKeeper;
    private static final int SESSION_OUT = 30000;
    private Watcher watcher = new Watcher(){

        @Override
        public void process(WatchedEvent watchedEvent) {
            System.out.println("process = [" + watchedEvent.getType() + "]");
        }
    };
    @Before
    public void setUp() throws IOException {
        zooKeeper = new ZooKeeper("192.168.85.128:2181",SESSION_OUT,watcher);
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        zooKeeper.create("/node", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getNodeData() throws Exception {
        String value = new String(zooKeeper.getData("/node",null, null));
        System.out.println(value);
    }

    @Test
    public void setNodeData() throws Exception {
        zooKeeper.setData("/node","test1".getBytes(), -1);
    }

    @Test
    public void delete() throws Exception {
        zooKeeper.delete("/node",-1);
    }

    @Test
    public void exists() throws Exception {
        Stat stat = zooKeeper.exists("/node", null);
        System.out.println(stat.getCzxid());
    }

    @After
    public void tearDown() throws InterruptedException {
        zooKeeper.close();
    }
}
