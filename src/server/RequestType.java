package server;

public enum RequestType {
    BEGIN_XACTION,
    END_XACTION,
    INSERT,
    DELETE,
    LOOK_UP,
    UPDATE,
    RANGE,
    ROLLBACK,
    COMMIT
}
