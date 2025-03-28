package trading.exchange;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ZooKeeperLeadershipManager {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperLeadershipManager.class);

    private final LeaderLatch leaderLatch;
    private final ExecutorService singleThreadExecutor;

    private final List<Runnable> leadershipAcquiredTasks = new ArrayList<>();
    private final List<Runnable> leadershipLostTasks = new ArrayList<>();

    public ZooKeeperLeadershipManager() throws UnknownHostException {
        String zooKeeperCluster = "127.0.0.1:2181";
        int zooKeeperSessionTimeoutMs = 3_000;
        int zooKeeperConnectionTimeoutMs = 3_000;
        int zooKeeperExponentialBackoffRetryBaseSleepMs = 1_000;
        log.info("init. curatorFramework");
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zooKeeperCluster)
                .sessionTimeoutMs(zooKeeperSessionTimeoutMs)
                .connectionTimeoutMs(zooKeeperConnectionTimeoutMs)
                .retryPolicy(new ExponentialBackoffRetry(zooKeeperExponentialBackoffRetryBaseSleepMs,
                        Integer.MAX_VALUE /* Wait Zookeeper cluster to be back online */, Integer.MAX_VALUE))
                .build();
        client.start();

        String latchPath = "/trading-exchange/leader";
        log.info("init. leaderLatch. Path [{}]", latchPath);
        this.leaderLatch = new LeaderLatch(client, latchPath, InetAddress.getLocalHost().getHostName());

        singleThreadExecutor = Executors.newSingleThreadExecutor();
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info("leaderLatch. isLeader");
                executeLeadershipAcquiredTasks();
                log.info("leaderLatch. isLeader callback finished");
            }

            @Override
            public void notLeader() {
                log.info("leaderLatch. notLeader");
                executeLeadershipLostTasks();
                log.info("leaderLatch. notLeader callback finished");
            }
        }, singleThreadExecutor);
        log.info("init");
    }

    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }

    public void start() {
        try {
            leaderLatch.start();
            log.info("leaderLatch started");
        } catch (Exception e) {
            log.info("leaderLatch start failed", e);
            throw new RuntimeException(e);
        }
    }

    public void preDestroy() {
        log.info("preDestroy");
        singleThreadExecutor.shutdown();
        try {
            if (!singleThreadExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
                log.error("executorService shutdown timed out");
            } else {
                log.info("executorService stopped");
            }
        } catch (InterruptedException e) {
            log.error("preDestroy. Was Interrupted", e);
            throw new RuntimeException(e);
        }
    }


    public final synchronized void onLeadershipAcquired(Runnable task) {
        leadershipAcquiredTasks.add(task);
    }

    public final synchronized void onLeadershipLost(Runnable task) {
        leadershipLostTasks.add(task);
    }

    protected final synchronized void executeLeadershipAcquiredTasks() {
        log.info("executeLeadershipAcquiredTasks. Executing [{}] tasks", leadershipAcquiredTasks.size());
        leadershipAcquiredTasks.forEach(Runnable::run);
    }

    protected final synchronized void executeLeadershipLostTasks() {
        log.info("executeLeadershipLostTasks. Executing [{}] tasks", leadershipLostTasks.size());
        leadershipLostTasks.forEach(Runnable::run);
    }

}