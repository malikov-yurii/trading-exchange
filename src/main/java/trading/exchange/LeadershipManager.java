package trading.exchange;

public interface LeadershipManager {

    void onLeadershipAcquired(Runnable task);

    void onLeadershipLost(Runnable task);

    boolean hasLeadership();

    default boolean isFollower() {
        return !hasLeadership();
    }
}
