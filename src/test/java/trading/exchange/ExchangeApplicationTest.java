package trading.exchange;

import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

class ExchangeApplicationTest {

    @Test
    void main() throws UnknownHostException {
        ExchangeApplication.main(new String[]{"arg1", "arg2"});
        System.out.println("Test Trading Exchange Application Started...");
        assertThat(true).isTrue();
    }
}