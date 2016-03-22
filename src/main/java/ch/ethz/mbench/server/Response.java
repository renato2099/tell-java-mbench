package ch.ethz.mbench.server;

/**
 * Handles response messages to be sent to clients
 */
public class Response {

    private final long responseTime;
    private final long numRecords;
    private ClientSession clientSession;

    public Response(long responseTime) {
        this.responseTime = responseTime;
        this.numRecords = 0;
    }
    public Response(long responseTime, long numRecords) {
        this.responseTime = responseTime;
        this.numRecords = numRecords;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public void setClientSession(ClientSession clientSession) {
        this.clientSession = clientSession;
    }

    public ClientSession getClientSession() {
        return clientSession;
    }
}
