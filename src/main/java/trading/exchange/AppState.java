package trading.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppState {
    private static final Logger log = LoggerFactory.getLogger(AppState.class);

    private final LeadershipManager leadershipManager;

    private State state = State.INITIALIZING;

    public AppState(LeadershipManager leadershipManager) {
        this.leadershipManager = leadershipManager;
    }

    public boolean isRecoveredLeader() {
        return leadershipManager.hasLeadership() && state == State.RECOVERED;
    }

    public boolean isNotRecoveredLeader() {
        return !isRecoveredLeader();
    }

    public void setRecovering() {
        setState(State.RECOVERING);
    }

    public void setRecovered() {
        setState(State.RECOVERED);
    }

    private void setState(State newState) {
        this.state = newState;
        log.info("State changed to {}", this.state);
    }

    enum State {
        INITIALIZING,
        RECOVERING,
        RECOVERED
    }

}
