package trading.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AppState {
    private static final Logger log = LoggerFactory.getLogger(AppState.class);

    private final LeadershipManager leadershipManager;

    private State state = State.INITIALIZING;
    private List<Runnable> onAppRecovered = new ArrayList<>();

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
        if (state != State.RECOVERED) {
            setState(State.RECOVERED);
            this.onAppRecovered.forEach(Runnable::run);
        }
    }

    private void setState(State newState) {
        this.state = newState;
        log.info("State changed to {}", this.state);
    }

    public void onRecovered(Runnable onAppRecoveredTask) {
        this.onAppRecovered.add(onAppRecoveredTask);
    }

    enum State {
        INITIALIZING,
        RECOVERING,
        RECOVERED
    }

}
