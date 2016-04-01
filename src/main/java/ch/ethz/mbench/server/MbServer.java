package ch.ethz.mbench.server;

import org.apache.commons.cli.*;
import org.apache.commons.httpclient.util.IdleConnectionTimeoutThread;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

/**
 */
public abstract class MbServer {

    public static Map<SelectionKey, ClientSession> clientMap = new HashMap<>();
    public static Logger Log = Logger.getLogger(MbServer.class);
    public static long clientIds;
    private static String LOCALHOST = "0.0.0.0";
    private short serverPort;
    private short numAsioThreads;
    private short nCols;
    private short scaleFactor;
    // server threads
    private ExecutorService service;
    private List<Future<Response>> futures;
    // nio
    private Selector selector;
    private SelectionKey serverKey;
    private ServerSocketChannel serverChannel;

    public void initialize() {
        service = Executors.newFixedThreadPool(numAsioThreads, new ServiceThreadFactory());
        futures = new ArrayList<>();
    }

    public void run() throws IOException, ExecutionException, InterruptedException {
        // start server threads
        initialize();
        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(LOCALHOST, serverPort)).configureBlocking(false);
        selector = Selector.open();
        serverKey = serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        try {
            System.out.println("Started mbench server");
            while (true)
                loop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void loop() throws Throwable {
        // process incoming requests
        selector.selectNow();
        for (SelectionKey key : selector.selectedKeys()) {
            if (!key.isValid())
                continue;
            if (key.isReadable()) {
                ClientSession clieSession = clientMap.get(key);
                if (clieSession == null) {
                    continue;
                }
                processRequest(clieSession, key);
            }

            // handle a new client that connects
            if (key == serverKey) {
                SocketChannel acceptedChannel = serverChannel.accept();
                if (acceptedChannel == null) continue;
                acceptedChannel.configureBlocking(false);
                SelectionKey readKey = acceptedChannel.register(selector, SelectionKey.OP_READ);
                clientMap.put(readKey, new ClientSession(readKey, acceptedChannel, ++clientIds));
                Log.info("New client ip=" + acceptedChannel.getRemoteAddress() + ", nClients=" + clientMap.size());
                System.out.println("New client ip=" + acceptedChannel.getRemoteAddress() + ", nClients=" + clientMap.size());
            }
        }

        // send results of processed requests
        selector.selectedKeys().clear();
        List<Future<Response>> nextFutures = new ArrayList<>();
        for (Future<Response> future : futures) {
            if (future.isDone()) {
                Response resp = future.get();
                resp.getClientSession().writeResponse(resp.getResults());
            } else {
                nextFutures.add(future);
            }
        }
        // this is save because access to futures are single-threaded
        futures = nextFutures;
    }

    private void processRequest(ClientSession clieSession, SelectionKey key) {
        ServerCmd scm = clieSession.readCmd();
        if (scm == null) return;
        switch (scm.getType()) {
            case CREATE_SCHEMA:
                futures.add(service.submit(() -> {
                    ServiceThread serviceThread = (ServiceThread) Thread.currentThread();
                    Response resp = createSchema(nCols, serviceThread.connection);
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case POPULATE:
                futures.add(service.submit(() -> {
                    Object[] args = scm.getArgs();
                    ServiceThread serviceThread = (ServiceThread) Thread.currentThread();
                    Response resp = populate((Long) args[0], (Long) args[1], serviceThread.connection);
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case BATCH_OP:
                futures.add(service.submit(() -> {
                    Object[] args = scm.getArgs();
                    ServiceThread serviceThread = (ServiceThread) Thread.currentThread();
                    Response resp = doBatchOp((int) args[0], (double) args[1], (double) args[2], (double) args[3],
                            (int) args[4], (long) args[5], (long) args[6], (long) args[7], serviceThread.connection);
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case Q1:
                futures.add(service.submit(() -> {
                    ServiceThread serviceThread = (ServiceThread) Thread.currentThread();
                    Response resp = query1(serviceThread.connection);
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case Q2:
                futures.add(service.submit(() -> {
                    long responseTime = 0;
                    Response resp = new Response(new Object[]{true, "Q2 is not needed", responseTime});
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case Q3:
                futures.add(service.submit(() -> {
                    long responseTime = 0;
                    Response resp = new Response(new Object[]{true, "Q3 is not needed", responseTime});
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case DISCONNECT:
                clieSession.disconnect();
                break;
        }
    }

    private Response query1(Connection mConnection) {
        long t0 = System.nanoTime();

        Transaction tx = mConnection.startTx();
        long nTuples = tx.query1();
        boolean commitRes = tx.commit();
        long responseTime = System.nanoTime() - t0;
        String errorMsg = "";
        if (!commitRes)
            errorMsg = "Error:nTup=" + nTuples;
        Response resp = new Response(new Object[]{commitRes, errorMsg, responseTime});
        System.out.println("Query 1 response time: " + responseTime);
        return resp;
    }

    private Response doBatchOp(final int nOps, final double iProb, final double dProb, final double uProb, final int clientId,
                               final long nClients, long baseInsKey, long baseDelKey, final Connection mConnection) {
        double gProb = 1.0 - iProb - dProb - uProb;
        if (gProb < 0.0) {
            throw new RuntimeException("Probabilities sum up to negative number");
        }

        Map<Long, Tuple> inserts = new HashMap<>();
        Vector<Long> deletes = new Vector<>();
        Vector<UpdatePair> updates = new Vector<>();
        Vector<Long> getKeys = new Vector<>();
        double ops[] = new double[nOps];
        for (int i = 0; i < nOps; i++) {
            ops[i] = Tuple.getRandomDouble(0.0, 1.0);
            if (ops[i] < iProb) {
                //do insert
                baseInsKey += nClients;
                inserts.put(baseInsKey, Tuple.createInsert(nCols));
            } else if (ops[i] < iProb + uProb) {
                // do update
                long updKey = Tuple.rndKey(baseInsKey, baseDelKey, nClients, clientId);
                updates.add(new UpdatePair(updKey, Tuple.rndUpdate(nCols)));
            } else if (ops[i] < iProb + uProb + dProb) {
                if (baseDelKey + nClients >= baseInsKey) {
                    // do insert
                    ops[i] = -1.0;
                    baseInsKey += nClients;
                    inserts.put(baseInsKey, Tuple.createInsert(nCols));
                } else {
                    // do delete
                    deletes.add(baseDelKey);
                    baseDelKey += nClients;
                }
            } else {
                // do get
                getKeys.add(Tuple.rndKey(baseInsKey, baseDelKey, nClients, clientId));
            }
        }
        // do actual operations
        Iterator<Map.Entry<Long, Tuple>> insIter = inserts.entrySet().iterator();
        Iterator<Long> delIter = deletes.iterator();
        Iterator<UpdatePair> updIter = updates.iterator();
        Iterator<Long> getIter = getKeys.iterator();

        long t0 = System.nanoTime();
        int sucOps = 0;

        Transaction tx = mConnection.startTx();
        for (int i = 0; i < nOps; i++) {
            if (ops[i] < iProb) {
                //do insert
                Map.Entry<Long, Tuple> nextIns = insIter.next();
                if (tx.insert(nextIns.getKey(), nextIns.getValue()))
                    sucOps++;
            } else if (ops[i] < iProb + uProb) {
                // do update
                UpdatePair nextUpd = updIter.next();
                if (tx.update(nextUpd.key, nextUpd.tuple))
                    sucOps++;
            } else if (ops[i] < iProb + uProb + dProb) {
                // do delete
                if (tx.remove(delIter.next()))
                    sucOps++;
            } else {
                // do get
                if (tx.get(getIter.next()))
                    sucOps++;
            }
        }
        boolean commitRes = tx.commit();

        long responseTime = System.nanoTime() - t0;
        boolean success = commitRes && (nOps == sucOps);
        StringBuilder errorMsg = new StringBuilder();
        if (!success)
            errorMsg.append("ERROR:").append("suc=").append(sucOps).append("/").append(nOps);
        Response resp = new Response(new Object[]{success, errorMsg.toString(), baseInsKey, baseDelKey, responseTime});
        return resp;
    }

    public Response populate(long start, long end, Connection mConnection) {
        Transaction tx = mConnection.startTx();
        Map<Long, Tuple> inserts = new HashMap<>();
        long sucOps = 0;
        for (long i = start; i < end; ++i) {
            inserts.put(i, Tuple.createInsert(nCols));
        }
        long t0 = System.nanoTime();
        for (Map.Entry<Long, Tuple> ins : inserts.entrySet()) {
            if (tx.insert(ins.getKey(), ins.getValue()))
                sucOps++;
        }
        boolean commitRes = tx.commit();
        long responseTime = System.nanoTime() - t0;
        boolean success = commitRes && (end - start == sucOps);

        StringBuilder errorMsg = new StringBuilder();
        if (!success)
            errorMsg.append("ERROR:").append("suc=").append(sucOps).append("/").append(end - start);
        Response resp = new Response(new Object[]{success, errorMsg.toString(), responseTime});
        return resp;
    }

    public Response createSchema(int nCols, Connection mConnection) {
        mConnection.createSchema(nCols);
        Response resp = new Response(new Object[]{true, ""});
        return resp;
    }

    protected abstract Connection createConnection();

    public short getNCols() {
        return nCols;
    }

    public Options getCmdLineOptions() {
        Options options = new Options();
        options.addOption(Option.builder("h").argName("help").desc("Show help message").build());
        options.addOption(Option.builder("t").argName("threads").hasArg().desc("Number of asio threads").build());
        options.addOption(Option.builder("p").argName("port").hasArg().desc("Port to bind to").build());
        options.addOption(Option.builder("n").argName("num-columns").hasArg().desc("Number of columns of table").build());
        options.addOption(Option.builder("s").argName("scaling-factor").required(true).hasArg().desc("Scaling factor").build());
        return options;
    }

    public void parseCmdLine(String args[]) {
        CommandLine commandLine;
        CommandLineParser parser = new DefaultParser();
        Options options = getCmdLineOptions();
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption("h") | !commandLine.hasOption("s")) {
                new HelpFormatter().printHelp("mbench-server", options);
                System.exit(0);
            }
            numAsioThreads = Short.parseShort(commandLine.getOptionValue("t", "12"));
            serverPort = Short.parseShort(commandLine.getOptionValue("p", "8713"));
            nCols = Short.parseShort(commandLine.getOptionValue("n", "10"));
            scaleFactor = Short.parseShort(commandLine.getOptionValue("s"));
        } catch (ParseException exception) {
            System.out.print("Parse error: ");
            new HelpFormatter().printHelp("mbench-server", options);
            System.exit(0);
        }
    }

    /**
     * Interface to how connections are created
     */
    public interface Connection {

        Transaction startTx();

        void createSchema(int nCols);

    }

    /**
     * Interface to what transactions actually do
     */
    public interface Transaction {

        boolean insert(Long key, Tuple value);

        boolean commit();

        boolean update(Long key, Tuple value);

        boolean remove(Long key);

        boolean get(Long key);

        long query1();

    }

    // service thread keeps its own connection
    private class ServiceThread extends Thread {
        private final Connection connection;

        public ServiceThread(Runnable runnable, Connection connection) {
            super(runnable);
            this.connection = connection;
        }
    }

    // factory for service threads
    private class ServiceThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable runnable) {
            return new ServiceThread(runnable, createConnection());
        }
    }

    private class UpdatePair {

        private final long key;
        private final Tuple tuple;

        public UpdatePair(long key, Tuple tuple) {
            this.key = key;
            this.tuple = tuple;
        }
    }
}

