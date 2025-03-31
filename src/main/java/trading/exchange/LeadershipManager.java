package trading.exchange;

public interface LeadershipManager extends LeadershipStateProvider {

    void onLeadershipAcquired(Runnable task);

    void onLeadershipLost(Runnable task);

}
