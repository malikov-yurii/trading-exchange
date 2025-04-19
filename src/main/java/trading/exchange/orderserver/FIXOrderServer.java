package trading.exchange.orderserver;

import fix.PipeDelimitedScreenLogFactory;
import fix.ReusableMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Acceptor;
import quickfix.Application;
import quickfix.ApplicationAdapter;
import quickfix.Dictionary;
import quickfix.FieldNotFound;
import quickfix.FileStoreFactory;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketAcceptor;
import quickfix.UnsupportedMessageType;
import trading.api.OrderMessage;
import trading.api.OrderMessageSerDe;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.AsyncLogger;
import trading.common.LFQueue;
import trading.exchange.AppState;
import trading.exchange.LeadershipManager;
import trading.exchange.ReplayReplicationLogConsumer;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FIXOrderServer implements OrderServer {

    private static final Logger log = LoggerFactory.getLogger(FIXOrderServer.class);

    private final LFQueue<OrderRequest> clientRequests;
    private final LFQueue<OrderMessage> clientResponses;
    private final LeadershipManager leadershipManager;
    private final AppState appState;

    private ReplicationConsumer replicationConsumer;
    private RequestSequencer requestSequencer;
    private Thread requestSequencerThread;

    private final AtomicLong respSeqNum = new AtomicLong(1);
    private final Map<Long, SessionHolder> sessionsByClientId = new ConcurrentHashMap<>();
    record SessionHolder(SessionID sessionId, long clientId, Message reusableOutMessage) {}

    private long seqNum;
    private Acceptor acceptor;
    private final AsyncLogger asyncLogger;

    public FIXOrderServer(LFQueue<OrderRequest> clientRequests,
                          LFQueue<OrderMessage> clientResponses,
                          LeadershipManager leadershipManager,
                          AppState appState,
                          AsyncLogger asyncLogger) {
        this.asyncLogger = asyncLogger;
        this.clientRequests = clientRequests;
        this.clientResponses = clientResponses;
        this.leadershipManager = leadershipManager;
        this.appState = appState;
        this.clientResponses.subscribe(this::processResponse);
    }

    @Override
    public synchronized void start() {
        leadershipManager.onLeadershipAcquired(() -> {
            boolean wasRunning = stopReplicationConsumer();
            if (!wasRunning) {
                log.info("start. stopReplicationConsumer. wasRunning == false");
                appState.setRecovering();
                ReplayReplicationLogConsumer replayConsumer = new ReplayReplicationLogConsumer(clientRequests);
                replayConsumer.run();
                this.seqNum = replayConsumer.getLastSeqNum();
            }
            appState.setRecovered();
            startRequestSequencer();
            startFIXAcceptor();
        });

        leadershipManager.onLeadershipLost(() -> {
            stopFIXAcceptor();
            stopRequestSequencer();
            startReplicationConsumer();
        });
    }

    private void startFIXAcceptor() {
        try {
            // 1) figure out which IFC host we're on
            final String thisHost = System.getenv("THISHOST");
            final String dynamicSenderCompID = switch (thisHost) {
                case "exchange-1" -> "E1";
                case "exchange-2" -> "E2";
                case "exchange-3" -> "E3";
                default -> throw new IllegalStateException("Unknown host: " + thisHost);
            };

            // 2) Read the “template” config file (has exactly one [session] with SenderCompID=E1)
            SessionSettings template = new SessionSettings("fix-server.cfg");

            // 3) Pull out its default section...
            Dictionary defaultDict = template.get();  // this is the defaults
            // 4) And pull out its single real session section:
            Iterator<SessionID> iter = template.sectionIterator();
            if (!iter.hasNext()) {
                throw new IllegalStateException("No session in fix-server.cfg");
            }
            SessionID origSessionId = iter.next();
            Properties origSessionProps = template.getSessionProperties(origSessionId, /*includeDefaults=*/ true);

            // 5) Tweak that one session’s SenderCompID in the copy of its properties
            origSessionProps.setProperty(SessionSettings.SENDERCOMPID, dynamicSenderCompID);

            // 6) Build a brand new Settings object with only defaults + our one updated session
            SessionSettings settings = new SessionSettings();
            settings.set(defaultDict);  // populate defaults
            // create a new SessionID key for it:
            SessionID newSessionId = new SessionID(
                    origSessionId.getBeginString(),
                    dynamicSenderCompID,
                    origSessionId.getTargetCompID()
            );
            settings.set(newSessionId, new Dictionary(null, origSessionProps));

            log.info("{} | Starting FIX acceptor for session {}", thisHost, newSessionId);

            // 7) The rest is unchanged
            MessageStoreFactory storeFactory = new FileStoreFactory(settings);
            LogFactory logFactory       = new PipeDelimitedScreenLogFactory(asyncLogger);
            MessageFactory messageFactory = new ReusableMessageFactory(new Message());
            Application application     = new FIXApplicationAdapter();

            acceptor = new ThreadedSocketAcceptor(
                    application,
                    storeFactory,
                    settings,
                    logFactory,
                    messageFactory
            );
            acceptor.start();
            log.info("FIX acceptor started.");
        } catch (Exception e) {
            log.error("Failed to start FIX acceptor", e);
        }
    }

    private void stopFIXAcceptor() {
        if (acceptor != null) {
            acceptor.stop();
            acceptor = null;
            log.info("FIX acceptor stopped.");
        }
    }

    private synchronized void startReplicationConsumer() {
        replicationConsumer = new ReplicationConsumer(clientRequests, appState);
        replicationConsumer.run();
    }

    private synchronized boolean stopReplicationConsumer() {
        if (replicationConsumer == null) {
            log.info("stopReplicationConsumer. replicationConsumer == null");
            return false;
        }
        replicationConsumer.shutdown();
        this.seqNum = replicationConsumer.getLastSeqNum();
        replicationConsumer = null;
        return true;
    }

    private synchronized void startRequestSequencer() {
        requestSequencer = new RequestSequencer(clientRequests, seqNum, appState);
        requestSequencerThread = new Thread(requestSequencer, "RequestSequencerThread");
        requestSequencerThread.start();
    }

    private synchronized void stopRequestSequencer() {
        if (requestSequencer != null) {
            requestSequencer.shutdown();
        }
        if (requestSequencerThread != null && requestSequencerThread.isAlive()) {
            requestSequencerThread.interrupt();
            try {
                requestSequencerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        requestSequencerThread = null;
        requestSequencer = null;
    }

    private void processResponse(OrderMessage orderMessage) {
        SessionHolder sessionHolder = null;
        try {
            if (appState.isNotRecoveredLeader()) {
                if (log.isDebugEnabled()) {
                    log.debug("processResponse. isNotRecoveredLeader. Not publishing {}", orderMessage);
                }
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Start Sending {}", orderMessage);
            }
            if (orderMessage == null) {
                log.info("processResponse. orderMessage == null");
            }

            long seq = respSeqNum.getAndIncrement();
            orderMessage.setSeqNum(seq);

            Long clientId = orderMessage.getClientId();
            sessionHolder = sessionsByClientId.get(clientId);
            SessionID sessionId = sessionHolder.sessionId;
            if (sessionId == null) {
                log.warn("No session for clientId={}", clientId);
                return;
            }

            Message msg = sessionHolder.reusableOutMessage;
            OrderMessageSerDe.toFIXMessage(orderMessage, msg);

            Session.sendToTarget(msg, sessionId);
            if (log.isDebugEnabled()) {
                log.debug("Sending FIX response to clientId={}: {} {}", clientId,
                        msg.toString().replace('\u0001', '|'), orderMessage);
            }
        } catch (Exception e) {
            log.error("Failed to send FIX OrderMessage, " + orderMessage + " sessionHolder", e);
        }
    }

    public synchronized void shutdown() {
        stopFIXAcceptor();
        stopRequestSequencer();
    }

    private class FIXApplicationAdapter extends ApplicationAdapter {

        @Override
        public void onLogon(SessionID sessionId) {
            log.info("FIX session logged in: " + sessionId);
        }

        @Override
        public void onLogout(SessionID sessionId) {
            log.info("FIX session logged out: " + sessionId);
        }

        @Override
        public void fromApp(Message message, SessionID sessionId) {
            if (log.isDebugEnabled()) {
                log.debug("Received App Message: " + message.toString().replace('\u0001', '|'));
            }
            try {

                OrderRequest request = OrderRequestSerDe.getOrderRequest(message);

                if (log.isDebugEnabled()) {
                    log.debug("Parsed client request: {}", request);
                }

                long clientId = request.getClientId();
                sessionsByClientId.computeIfAbsent(clientId, clId -> {
                    log.info("Client ID {} registered for session {}", clientId, sessionId);
                    return new SessionHolder(sessionId, clientId, new Message());
                });


                if (requestSequencer != null) {
                    requestSequencer.process(request);
                } else {
                    log.warn("RequestSequencer not active, ignoring request from clientId={}", request);
                }

            } catch (Exception e) {
                log.error("fromApp. Failed " + message + " + " + sessionId, e);
            }
        }
    }

}
