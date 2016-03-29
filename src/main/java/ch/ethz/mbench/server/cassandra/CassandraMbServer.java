package ch.ethz.mbench.server.cassandra;

import ch.ethz.mbench.server.MbServer;
import ch.ethz.mbench.server.Tuple;
import com.datastax.driver.core.querybuilder.*;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.*;
import org.apache.log4j.Logger;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * Cassandra implementation of micro-benchmark
 */
public class CassandraMbServer extends MbServer {

    private static final int HIGHER_TIMEOUT = 360000000;
    public static final String CONTAINER = "mbench";
    private static final String REPL_FACTOR = "1";
    public static final String TABLE_NAME = "maintable";
    // cassandra ring properties
    private String clNode;
    private String nodePort;
    public static Logger Log = Logger.getLogger(CassandraMbServer.class);

    public static void main(String args[]) {
        CassandraMbServer mserver = new CassandraMbServer();
        mserver.parseCmdLine(args);
        try {
            mserver.run();
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Connection createConnection() {
        return new CassandraConnection(clNode, nodePort);
    }

    /**
     * Connection to Cassandra
     */
    public static class CassandraConnection implements Connection {

        private Cluster cluster;

        CassandraConnection(String node, String port) {
            cluster = Cluster.builder().withPort(Integer.parseInt(port))
                    .addContactPoint(node).build();
            cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(HIGHER_TIMEOUT).setReadTimeoutMillis(HIGHER_TIMEOUT);
            Metadata md = cluster.getMetadata();
            Log.info(String.format("Connected to: %s\n", md.getClusterName()));
            for (Host h : md.getAllHosts()) {
                Log.debug(String.format("Datatacenter: %s; Host: %s; Rack: %s\n",
                        h.getDatacenter(), h.getAddress(), h.getRack()));
            }
        }

        @Override
        public Transaction startTx() {
            return new CassandraTransaction(cluster);
        }

        @Override
        public void createSchema(int nCols) {
            Session session = cluster.connect();
            // create keyspace
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE KEYSPACE ").append(CONTAINER).append(" WITH replication ");
            sb.append("= {'class':'SimpleStrategy', 'replication_factor':");
            sb.append(REPL_FACTOR).append("}");
            sb.append("AND DURABLE_WRITES = false").append(";");
            try {
                session.execute(sb.toString());
                Log.warn("KeySpace created!");
            } catch (com.datastax.driver.core.exceptions.AlreadyExistsException ex) {
                Log.warn("Keyspace already exists!");
            }

            // create table
            sb.setLength(0);
            sb.append("CREATE TABLE ").append(CONTAINER).append(".").append(TABLE_NAME);
            sb.append("(id bigint,");
            for (int i = 0; i < nCols; i++) {
                sb.append("A").append(i % 10).append(" ");
                switch (i) {
                    case 0:
                    case 7:
                        sb.append("double,");
                        break;
                    case 1:
                    case 2:
                        sb.append("int,");
                        break;
                    case 3:
                    case 4:
                        sb.append("int,");
                        break;
                    case 5:
                    case 6:
                        sb.append("bigint,");
                        break;
                    case 8:
                    case 9:
                        sb.append("varchar,");
                        break;
                }
            }
//            sb.append("PRIMARY KEY ((id), A3)");
            sb.append("PRIMARY KEY (id)");
            sb.append(");");

            try {
                session.execute(sb.toString());
                Log.warn("Table created!");
                session.close();
            } catch (com.datastax.driver.core.exceptions.AlreadyExistsException ex) {
                Log.warn("Table already exists!");
            }
        }
    }

    /**
     * Handles operations in Cassandra
     */
    public static class CassandraTransaction implements Transaction {
        private final Session session;
        private Batch batch;

        CassandraTransaction(Cluster cluster) {
            session = cluster.connect(CONTAINER);
            batch = QueryBuilder.batch();
        }

        @Override
        public boolean insert(Long key, Tuple value) {
            boolean result = false;
            try {
                // setting field names up
                String fieldNames[] = new String[1 + value.getFieldNames().length];
                fieldNames[0] = "id";
                System.arraycopy(value.getFieldNames(), 0, fieldNames, 1, value.getFieldNames().length);
                // setting values up
                Object fieldValues[] = new Object[1 + value.getFieldValues().length];
                fieldValues[0] = key;
                System.arraycopy(value.getFieldValues(), 0, fieldValues, 1, value.getFieldValues().length);
                // preparing statement
                RegularStatement insert = QueryBuilder.insertInto(TABLE_NAME).values(fieldNames, fieldValues);
                // is this the right way to set consistency level for Batch?
                insert.setConsistencyLevel(ConsistencyLevel.ANY);
                batch.add(insert);
                result = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean commit() {
            boolean result = false;
            try {
                if (batch != null) {
                    session.execute(batch);
                    session.close();
                    result = true;
                } else
                    throw new RuntimeException("No operations to be committed!");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean update(Long key, Tuple value) {
            boolean result = false;
            try {
                Update update = QueryBuilder.update(CONTAINER, TABLE_NAME);

                for (int i = 0; i < value.getNumFields(); i++) {
                    if (value.getFieldValues() != null)
                        update.with(set(value.getFieldNames()[i], value.getFieldValues()[i]));
                }
                update.where(eq("id", key));
                batch.add(update);
                result = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean remove(Long key) {
            boolean result = false;
            try {
                Delete.Where delete = QueryBuilder.delete().
                        from(CONTAINER, TABLE_NAME).where(eq("id", key));
                delete.setConsistencyLevel(ConsistencyLevel.ANY);
                batch.add(delete);
                result = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean get(Long key) {
            boolean result = false;
            try {
                RegularStatement get = QueryBuilder.select()
                        .all()
                        .from(CONTAINER, TABLE_NAME)
                        .where(eq("id", key));
                batch.add(get);
                result = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public long query1() {
            Select count = QueryBuilder.select().column("a0").countAll().from(CONTAINER, TABLE_NAME);
            ResultSet rs = session.execute(count);
            return rs.all().size();
        }
    }

    @Override
    public void parseCmdLine(String args[]) {
        super.parseCmdLine(args);
        CommandLine commandLine;
        CommandLineParser parser = new DefaultParser();
        Options options = getCmdLineOptions();
        try {
            commandLine = parser.parse(options, args);
            clNode = commandLine.getOptionValue("cn");
            nodePort = commandLine.getOptionValue("np", "9042");
        } catch (ParseException exception) {
            Log.error("Parse error: ");
            new HelpFormatter().printHelp("mbench-server", options);
            System.exit(0);
        }
    }

    @Override
    public Options getCmdLineOptions() {
        Options options = super.getCmdLineOptions();
        options.addOption(Option.builder("cn").argName("cassandra-master").hasArg().required(true).desc("Cassandra node ip").build());
        options.addOption(Option.builder("np").argName("cassandra-port").hasArg().desc("Cassandra port").build());
        return options;
    }
}
