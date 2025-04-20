package trading.participant.ordergateway;

import fix.PipeDelimitedScreenLogFactory;
import fix.ReusableMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.DoNotSend;
import quickfix.FileStoreFactory;
import quickfix.Initiator;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketInitiator;
import quickfix.field.MsgType;
import trading.api.OrderMessage;
import trading.api.OrderMessageSerDe;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.AsyncLogger;
import trading.common.LFQueue;
import trading.participant.strategy.TradeEngineUpdate;

import java.util.concurrent.atomic.AtomicReference;


public class FIXOrderGatewayClient extends MessageCracker implements OrderGatewayClient, Application {
    private static final Logger log = LoggerFactory.getLogger(FIXOrderGatewayClient.class);

    public static final int SLEEP_MILLIS = 250;

    private final LFQueue<TradeEngineUpdate> tradeEngineUpdates;
    private final AtomicReference<Session> activeSession = new AtomicReference<>();
    private Initiator initiator;
    private final ThreadLocal<TradeEngineUpdate> tradeEngineUpdateThreadLocal =
            ThreadLocal.withInitial(() -> new TradeEngineUpdate(TradeEngineUpdate.Type.ORDER_MESSAGE));

    private final Message fixMessage = new Message();

    public FIXOrderGatewayClient(LFQueue<OrderRequest> orderRequests,
                                 LFQueue<TradeEngineUpdate> tradeEngineUpdates, AsyncLogger asyncLogger) {
        this.tradeEngineUpdates = tradeEngineUpdates;
        orderRequests.subscribe(this::sendOrderRequest);

        try {
            SessionSettings settings = new SessionSettings("fix-client.cfg");
            log.info("Connecting to FIX server with settings: {}", settings);
            Application app = this;
            MessageStoreFactory storeFactory = new FileStoreFactory(settings);
//            LogFactory logFactory = new ScreenLogFactory(true, true, true);
            LogFactory logFactory = new PipeDelimitedScreenLogFactory(asyncLogger);
            MessageFactory messageFactory = new ReusableMessageFactory(new Message());
            initiator = new ThreadedSocketInitiator(app, storeFactory, settings, logFactory, messageFactory);
            initiator.start();
            log.info("FIX client started successfully");
        } catch (Exception e) {
            log.error("Failed to start FIX client", e);
        }
    }

    @Override
    public void sendOrderRequest(OrderRequest request) {
        try {
            OrderRequestSerDe.toFIXMessage(request, fixMessage);

            trySend(fixMessage);

            if (log.isDebugEnabled()) {
                log.debug("Sent FIX request: {} for {}", fixMessage.toString().replace('\u0001', '|'), request);
            }
        } catch (IllegalArgumentException e) {
            log.error("Failed to send FIX message. " + request, e);
        } catch (Exception e) {
            log.error("Failed to send FIX message. Retrying. " + request, e);
            sendOrderRequest(request);
        }
    }

    private void trySend(Message fixMessage) {
        Session session = getActiveSession();

        boolean success = session.send(fixMessage);

        if (!success) {
            log.error("Unsuccessful send FIX message. Retrying. {}", fixMessage);
            trySend(fixMessage);
        }
    }

    private Session getActiveSession() {
        while (true) {
            Session session = this.activeSession.get();
            if (session == null) {
                log.warn("No activeSession set yet. Waiting {}ms", SLEEP_MILLIS);
            } else if (session.isLoggedOn()) {
                if (log.isDebugEnabled()) {
                    log.debug("FIX session is ready: {}", this.activeSession);
                }
                return session;
            } else {
                log.warn("Session not logged on yet: {}. Waiting {}ms", this.activeSession, SLEEP_MILLIS);
            }

            try {
                Thread.sleep(SLEEP_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted while waiting for FIX session", e);
            }
        }
    }

    @Override
    public void shutdown() {
        if (initiator != null) {
            initiator.stop();
        }
    }

    @Override
    public void start() {
        // Already started in constructor
    }

    @Override
    public void fromApp(Message message, SessionID sessionId) {
//        String fixMsg = message.toString().replace('\u0001', '|');
//        log.info("Received App Message: {}", fixMsg);
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            if (MsgType.EXECUTION_REPORT.equals(msgType)
                    || MsgType.ORDER_CANCEL_REJECT.equals(msgType)) {

                TradeEngineUpdate tradeEngineUpdate = tradeEngineUpdateThreadLocal.get();
                OrderMessage orderMessage = tradeEngineUpdate.getOrderMessage();

                OrderMessageSerDe.toOrderMessage(message, orderMessage);
//                log.info("Received orderMessage: {}. FIX: {}", orderMessage, fixMsg);

                tradeEngineUpdates.offer(tradeEngineUpdate);
            }
        } catch (Exception e) {
            log.error("Failed to parse ExecutionReport", e);
        }
    }

    @Override
    public void onCreate(SessionID sessionId) {
        log.info("Session created: {}", sessionId);
    }

    @Override
    public void onLogon(SessionID sessionId) {
        try {
            log.info("Logon: {}", sessionId);
            Session session = Session.lookupSession(sessionId);
            if (session == null) {
                log.error("onLogon. session is NULL");
                return;
            }
            log.info("Logon successful: {}. {}", sessionId, session);

            if (isPrimary(sessionId)) {
                Session prevSession = this.activeSession.getAndSet(session);
                log.info("onLogon. Updated activeSession from [{}] to PRIMARY SessionID: [{}]",
                        prevSession, session.getSessionID());
            }

            boolean wasUpdated = this.activeSession.compareAndSet(null, session);
            if (wasUpdated) {
                log.info("onLogon. Updated activeSession from NULL to SessionID: {}", session.getSessionID());
            }
            //TODO Client must send his client id on logon to cover edge cases: e.g. when requests are published to another instance
        } catch (Exception e) {
            log.error("Failed to logon to FIX session: " + sessionId, e);
        }
    }

    private boolean isPrimary(SessionID sessionId) {
        return "E1".equals(sessionId.getTargetCompID());
    }

    @Override
    public void onLogout(SessionID sessionId) {
        Session ses = this.activeSession.get();

        log.info("onLogout: {}. this.sessionID: {}", sessionId, ses);

        if (ses == null || sessionId.equals(ses.getSessionID())) {

            boolean wasUpdated = this.activeSession.compareAndSet(ses, null);
            if (wasUpdated) {
                log.info("onLogout. Updated activeSession from {} to NULL", ses);
            } else {
                log.info("onLogout. Not Updated activeSession from {} to NULL. Current {}", ses, this.activeSession);
                return;
            }

            log.info("Attempting failover to backup session...");
            for (SessionID sid : initiator.getSessions()) {
                if (!sid.equals(sessionId)) {
                    log.info("Available session: {}", sid);
                    Session session = Session.lookupSession(sid);
                    if (session != null) {
                        log.info("Found backup session: {}", sid);
                        // This session will reconnect automatically via the Initiator
                        // Wait until it logs on
                        waitForSessionReady(sid);
                        wasUpdated = this.activeSession.compareAndSet(null, session);
                        if (wasUpdated) {
                            log.info("Failed over to {}", session);
                        } else {
                            log.info("Not Failed over to {}. Current this.activeSession: {}", session,
                                    this.activeSession.get());
                        }
                        break;
                    }
                }
            }
            log.info("Failed failover to backup session...");
        }
    }

    private void waitForSessionReady(SessionID sid) {
        while (!Thread.currentThread().isInterrupted()) {
            Session session = Session.lookupSession(sid);
            if (session != null && session.isLoggedOn()) {
                log.info("Backup session is ready: {}", sid);
                break;
            }
            try {
                log.info("Waiting for backup FIX session {} to be ready...", sid);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId) {
//        String fixMsg = message.toString().replace('\u0001', '|');
//        log.info("fromAdmin. Received App Message: {}", fixMsg);
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            if (MsgType.EXECUTION_REPORT.equals(msgType)
                    || MsgType.ORDER_CANCEL_REJECT.equals(msgType)
                    || MsgType.REJECT.equals(msgType)) {

                TradeEngineUpdate tradeEngineUpdate = tradeEngineUpdateThreadLocal.get();
                OrderMessage orderMessage = tradeEngineUpdate.getOrderMessage();

                OrderMessageSerDe.toOrderMessage(message, orderMessage);
//                log.info("fromAdmin. Received orderMessage: {}. FIX: {}", orderMessage, fixMsg);

                tradeEngineUpdates.offer(tradeEngineUpdate);
            }
        } catch (Exception e) {
            log.error("fromAdmin. Failed to parse ExecutionReport", e);
        }
    }

    @Override
    public void toApp(Message message, SessionID sessionId) throws DoNotSend {
    }
}
