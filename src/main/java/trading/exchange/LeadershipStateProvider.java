package trading.exchange;

public interface LeadershipStateProvider {

    boolean hasLeadership();

    default boolean isFollower() {
        return !hasLeadership();
    }

}
