package aeron;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;

public final class AeronClient {

    public static final Aeron AERON_INSTANCE_REMOTE = initAeronRemote();
    public static final Aeron AERON_INSTANCE_LOCAL = initAeronLocal();

    public static Aeron initAeronRemote() {
        Aeron.Context aeronCtx;
        String aeronDir = AeronUtils.getAeronDirRemote();

        aeronCtx = new Aeron.Context().aeronDirectoryName(aeronDir);

        return Aeron.connect(aeronCtx);
    }

    public static Aeron initAeronLocal() {
        Aeron.Context aeronCtx;
        String aeronDir = AeronUtils.getAeronDirLocal();

        MediaDriver.Context mediaCtx = new MediaDriver.Context().aeronDirectoryName(aeronDir);
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaCtx);

        aeronCtx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());
        return Aeron.connect(aeronCtx);
    }

}
