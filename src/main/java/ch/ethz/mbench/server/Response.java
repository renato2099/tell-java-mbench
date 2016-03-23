package ch.ethz.mbench.server;

/**
 * Handles response messages to be sent to clients
 */
public class Response {

    private Object[] results;
    private ClientSession clientSession;

    public Response(Object[] results) {
        this.results = results;
    }

    public void setResult(Object result, int i) {
        results[i] = result;
    }

    public Object[] getResults() {
        return results;
    }

    public void setClientSession(ClientSession clientSession) {
        this.clientSession = clientSession;
    }

    public ClientSession getClientSession() {
        return clientSession;
    }
}
