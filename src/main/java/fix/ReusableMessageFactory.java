package fix;

import quickfix.*;
import quickfix.field.MsgType;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * MessageFactory that reuses a single Message instance for select application-level messages.
 *
 * ⚠️ NOT thread-safe — only use from a single thread (e.g., fromApp thread).
 * ⚠️ Only reuses D, 8, F, 9. All others fall back to allocation.
 */
@NotThreadSafe
public class ReusableMessageFactory extends DefaultMessageFactory {

    private final Message reusableMessage;

    public ReusableMessageFactory(Message reusableMessage) {
        this.reusableMessage = reusableMessage;
    }

    @Override
    public Message create(String beginString, String msgType) {
        if (!isReusable(msgType)) {
            return super.create(beginString,msgType); // default alloc path
        }

        reusableMessage.getHeader().clear();
        reusableMessage.clear();
        reusableMessage.getTrailer().clear();
        reusableMessage.getHeader().setString(MsgType.FIELD, msgType);

        return reusableMessage;
    }

    private boolean isReusable(String msgTypeStr) {
        if (msgTypeStr.length() != 1){
            return false;
        }

        return switch (msgTypeStr.charAt(0)) {
            case 'D', '8', 'F', '9' -> true;
            default -> false;
        };
    }
}