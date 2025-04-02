package trading.common;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;

import static trading.common.Utils.env;

public final class AeronClient {

    public static final Aeron INSTANCE = initAeronClient();

    public static Aeron initAeronClient() {
        Aeron.Context aeronCtx;
        String env = env("AERON_DIR", null);
        if (env != null) {
            aeronCtx = new Aeron.Context().aeronDirectoryName(env);
        } else {
            MediaDriver.Context mediaCtx = new MediaDriver.Context();
            MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaCtx);

            aeronCtx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());
        }
        return Aeron.connect(aeronCtx);
    }

}
