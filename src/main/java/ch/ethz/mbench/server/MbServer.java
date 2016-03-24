package ch.ethz.mbench.server;

import org.apache.commons.cli.*;
import org.apache.commons.collections.iterators.ObjectArrayIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

/**
 */
public abstract class MbServer {

    private static String LOCALHOST = "127.0.0.1";

    private short serverPort;
    private short numAsioThreads;
    private short nCols;
    private short scaleFactor;
    // server threads
    private ExecutorService service;
    private Queue<Future<Response>> futures;
    private Connection mConnection;

    // nio
    private Selector selector;
    private SelectionKey serverKey;
    private ServerSocketChannel serverChannel;
    public static Map<SelectionKey, ClientSession> clientMap = new HashMap<>();
    public static Logger Log = Logger.getLogger(MbServer.class);

    public void initialize() {
        service = Executors.newFixedThreadPool(numAsioThreads);
        futures = new ConcurrentLinkedQueue<>();
        mConnection = createConnection();
        mConnection.createSchema(nCols);
    }

    public void run() throws IOException, ExecutionException, InterruptedException {
        // start server threads
        initialize();
        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(LOCALHOST, serverPort)).configureBlocking(false);
        selector = Selector.open();
        serverKey = serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                loop();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    public void loop() throws Throwable {
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

            //if (key.isAcceptable()) {
            if (key == serverKey) {
                SocketChannel acceptedChannel = serverChannel.accept();
                if (acceptedChannel == null) continue;
                acceptedChannel.configureBlocking(false);
                SelectionKey readKey = acceptedChannel.register(selector, SelectionKey.OP_READ);
                clientMap.put(readKey, new ClientSession(readKey, acceptedChannel));
                Log.info("New client ip=" + acceptedChannel.getRemoteAddress() + ", nClients=" + clientMap.size());
            }
        }
        selector.selectedKeys().clear();
        while (!futures.isEmpty()) {
            Future<Response> future = futures.poll();
            Response resp = future.get();
            resp.getClientSession().writeResponse(resp.getResults());
        }
    }

    private void processRequest(ClientSession clieSession, SelectionKey key) {
        ServerCmd scm = clieSession.readCmd();
        if (scm == null) return;
        switch (scm.getType()) {
            case CREATE_SCHEMA:
                futures.add(service.submit(() -> {
                    // TODO: add schema creation here if necessary, otherwise don't do anything
                    Response resp = new Response(new Object[] {true, ""});
                    return resp;
                }));
                break;
            case POPULATE:
                futures.add(service.submit(() -> {
                    Object[] args = scm.getArgs();
                    Response resp = populate((Long) args[0], (Long) args[1]);
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case BATCH_OP:
                futures.add(service.submit(() -> {
                    Object[] args = scm.getArgs();
                    Response resp = doBatchOp((double) args[0], (double) args[1], (double) args[2],
                            (int) args[3], (long) args[4], (long) args[5], (int) args[6], (int) args[7]);
                    resp.setClientSession(clieSession);
                    return resp;
                }));
                break;
            case Q1:
                futures.add(service.submit(() -> {
                    // TODO: implement
                    long responseTime = 0;
                    Response resp = new Response(new Object[] {true, "", responseTime});
                    return resp;
                }));
                break;
            case Q2:
                futures.add(service.submit(() -> {
                    long responseTime = 0;
                    Response resp = new Response(new Object[] {false, "Q2 is not implemented", responseTime});
                    return resp;
                }));
                break;
            case Q3:
                futures.add(service.submit(() -> {
                    long responseTime = 0;
                    Response resp = new Response(new Object[] {false, "Q3 is not implemented", responseTime});
                    return resp;
                }));
                break;
            case DISCONNECT:
                clieSession.disconnect();
                break;
        }
    }

    protected Response doBatchOp(double iProb, double dProb, double uProb,
                                 int nOps, long baseDelKey, long baseInsKey,
                                 int clientId, int nClients) {
        double gProb = 1.0 - iProb - dProb - uProb;
        if (gProb < 0.0) {
            throw new RuntimeException("Probabilities sum up to negative number");
        }

        Map<Long, Tuple> inserts = new HashMap<>();
        Vector<Long> deletes = new Vector<>();
        Map<Long, Tuple> updates = new HashMap<>();
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
                updates.put(updKey, Tuple.rndUpdate(nCols));
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
        Iterator<Map.Entry<Long, Tuple>> updIter = updates.entrySet().iterator();
        Iterator<Long> getIter = getKeys.iterator();

        long t0 = System.nanoTime();

        Transaction tx = mConnection.startTx();
        for (int i = 0; i < nOps; i++) {
            if (ops[i] < iProb) {
                //do insert
                Map.Entry<Long, Tuple> nextIns = insIter.next();
                tx.insert(nextIns.getKey(), nextIns.getValue());
            } else if (ops[i] < iProb + uProb) {
                // do update
                Map.Entry<Long, Tuple> nextUpd = updIter.next();
                tx.update(nextUpd.getKey(), nextUpd.getValue());
            } else if (ops[i] < iProb + uProb + dProb) {
                // do delete
                tx.remove(delIter.next());
            } else {
                // do get
                tx.get(getIter.next());
            }
        }
        tx.commit();

        long responseTime = System.nanoTime() - t0;
        // TODO: handle error case correctly (now we assume everything went fine)
        boolean success = true;
        String errorMsg = "";
        // TODO: return the correct keys here (baseInsertKey is the biggest key currently in the database for the
        // TODO: corresponding client-id and baseDeleteKey ist the smallest key currently present)
        long baseInsertKey = 0, baseDeleteKey = 0;
        return new Response(new Object[] {success, errorMsg, baseInsertKey, baseDeleteKey, responseTime});
    }

    public Response populate(long start, long end) {
        Transaction tx = mConnection.startTx();
        start = 0;
        end = 10;
        Map<Long, Tuple> inserts = new HashMap<>();
        for (long i = start; i < end; ++i) {
            inserts.put(start, Tuple.createInsert(nCols));
        }
        long t0 = System.nanoTime();
        for (Map.Entry<Long, Tuple> ins : inserts.entrySet()) {
            tx.insert(ins.getKey(), ins.getValue());
        }
        tx.commit();
        long responseTime = System.nanoTime() - t0;
        // TODO: handle error case correctly (now we assume everything went fine)
        boolean success = true;
        String errorMsg = "";
        return new Response(new Object[] {success, errorMsg, responseTime});

    }

    protected abstract Connection createConnection();

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
        void insert(Long key, Tuple value);

        void commit();

        void update(Long key, Tuple value);

        void remove(Long key);

        void get(Long key);
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
            numAsioThreads = Short.parseShort(commandLine.getOptionValue("t", "2"));
            serverPort = Short.parseShort(commandLine.getOptionValue("p", "8713"));
            nCols = Short.parseShort(commandLine.getOptionValue("n", "10"));
            scaleFactor = Short.parseShort(commandLine.getOptionValue("s"));
        } catch (ParseException exception) {
            System.out.print("Parse error: ");
            new HelpFormatter().printHelp("mbench-server", options);
            System.exit(0);
        }
    }
}

