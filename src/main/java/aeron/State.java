package aeron;

public enum State {
    AERON_READY,
    ARCHIVE_READY,
    POLLING_SUBSCRIPTION,
    SHUTTING_DOWN
}
