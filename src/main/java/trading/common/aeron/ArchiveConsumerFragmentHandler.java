package trading.common.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveConsumerFragmentHandler implements FragmentHandler {
    private static final Logger log = LoggerFactory.getLogger(ArchiveConsumerFragmentHandler.class);

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final var read = buffer.getLong(offset);
        log.info("Received read {}", read);
    }
}
