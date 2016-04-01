package ch.ethz.mbench.server.cassandra;

import ch.ethz.mbench.server.MbServer;
import ch.ethz.mbench.server.Tuple;
import com.datastax.driver.core.querybuilder.*;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.*;
import org.apache.log4j.Logger;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * Cassandra implementation of micro-benchmark
 */
public class CassandraMbServer extends MbServer {

    private static final int HIGHER_TIMEOUT = Integer.MAX_VALUE;//never timeout
    public static final String CONTAINER = "mbench";
    private static final String REPL_FACTOR = "1";
    public static final String TABLE_NAME = "maintable";
    // cassandra ring properties
    private String[] clNode;
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
        return new CassandraConnection(clNode, nodePort, this.getNCols());
    }


    /**
     * Connection to Cassandra
     */
    public static class CassandraConnection implements Connection {

        private static Cluster cluster = null;
        private Session session;
        private boolean sessionBound = false;
        private int nCols;
        PreparedStatement insertStmt, deleteStmt, updateStmt, query1Stmt, getStmt;

        CassandraConnection(String nodes[], String port, int nCols) {
            if (cluster == null) {
                cluster = Cluster.builder().withPort(Integer.parseInt(port)).addContactPoints(nodes).build();
                cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(HIGHER_TIMEOUT);
                cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(HIGHER_TIMEOUT);
            }
            session = cluster.connect();
            this.nCols = nCols;
            // insert
            insertStmt = session.prepare(doInsertString());
            // delete
            deleteStmt = session.prepare(doDeleteString());
            // get
            getStmt = session.prepare(doGetString());
            // query1
            query1Stmt = session.prepare(String.format("select max(a0) from %s.%s",CONTAINER, TABLE_NAME));
            query1Stmt.setConsistencyLevel(ConsistencyLevel.ALL);
        }

        private void rebindIfNecessary() {
            if (!sessionBound) {
                session = cluster.connect(CONTAINER);
                sessionBound = true;
            }
        }

        @Override
        public Transaction startTx() {
            rebindIfNecessary();
            return new CassandraTransaction(session, insertStmt, deleteStmt, updateStmt, query1Stmt, getStmt);
        }

        @Override
        public void createSchema(int nCols) {
            // create keyspace
            this.nCols = nCols;
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
//            sb.append("PRIMARY KEY ((id), A0)");
            sb.append("PRIMARY KEY (id)");
            sb.append(");");

            try {
                session.execute(sb.toString());
                Log.warn("Table created!");
            } catch (com.datastax.driver.core.exceptions.AlreadyExistsException ex) {
                Log.warn("Table already exists!");
            }
        }


        private  String doInsertString() {
            StringBuilder sb = new StringBuilder();
            StringBuilder params = new StringBuilder();
            params.append("?,");
            sb.append("INSERT INTO ").append(CONTAINER).append(".").append(TABLE_NAME).append("(").append("id,");
            for (int i = 0; i < nCols; i++) {
                sb.append("A").append(i % 10);
                params.append("?");
                if (i+1 < nCols) {
                    sb.append(",");
                    params.append(",");
                }
            }
            sb.append(")").append(" values (").append(params.toString()).append(")");
            return sb.toString();
        }

        private  String doDeleteString() {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(CONTAINER).append(".").append(TABLE_NAME).append(" WHERE id = ?");
            return sb.toString();
        }

        private  String doGetString() {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT * FROM ").append(CONTAINER).append(".").append(TABLE_NAME).append(" WHERE id = ?");
            return sb.toString();
        }

    }

    /**
     * Handles operations in Cassandra
     */
    public static class CassandraTransaction implements Transaction {
        private Session session;
        private BatchStatement batch;
        private PreparedStatement insertStmt, deleteStmt, updateStmt, query1Stmt, getStmt;
        private int batchCounter = 0;
        private static int MAX_BATCH_SIZE = 100;
        private Vector<BoundStatement> gets;

        CassandraTransaction(Session sess, PreparedStatement insertStmt, PreparedStatement deleteStmt,
                PreparedStatement updateStmt, PreparedStatement query1Stmt, PreparedStatement getStmt) {
            session = sess;
            this.insertStmt = insertStmt;
            this.deleteStmt = deleteStmt;
            this.updateStmt = updateStmt;
            this.getStmt = getStmt;
            this.query1Stmt = query1Stmt;
            batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            gets = new Vector();
        }

        //adds to batch and executes it if necessary
        private void addToBatch(Statement stmt) {
            batch.add(stmt);
            if (batchCounter++ >= MAX_BATCH_SIZE) {
                session.execute(batch);
                batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
                batchCounter = 0;
            }
        }

        @Override
        public boolean insert(Long key, Tuple value) {
            boolean result = false;
            try {
                QueryBuilder.insertInto(TABLE_NAME);
                // setting field names up
                String fieldNames[] = new String[1 + value.getFieldNames().length];
                fieldNames[0] = "id";
                System.arraycopy(value.getFieldNames(), 0, fieldNames, 1, value.getFieldNames().length);
                // setting values up
                Object fieldValues[] = new Object[1 + value.getFieldValues().length];
                fieldValues[0] = key;
                System.arraycopy(value.getFieldValues(), 0, fieldValues, 1, value.getFieldValues().length);
                // preparing statement
                BoundStatement insert = insertStmt.bind(fieldValues);
                // is this the right way to set consistency level for Batch?
                insert.setConsistencyLevel(ConsistencyLevel.ANY);
                addToBatch(insert);
                result = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean commit() {
            boolean getRes = true;
            try {
                if (gets != null && !gets.isEmpty()) {
                    for (BoundStatement get : gets) {
                        session.execute(get);
                    }
                }
                if (batch != null ) {
                    session.execute(batch);
                }
            } catch (Exception e) {
                e.printStackTrace();
                getRes = false;
            }
            return getRes;
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
//                addToBatch(update);
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
                BoundStatement delete = deleteStmt.bind(key);
                delete.setConsistencyLevel(ConsistencyLevel.ANY);
                addToBatch(delete);
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
//                addToBatch(getStmt.bind(key));
//                Select.Where get = QueryBuilder.select()
//                        .all()
//                        .from(CONTAINER, TABLE_NAME)
//                        .where(eq("id", key));
                gets.add(getStmt.bind(key));
                result = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public long query1() {

            ResultSet rs = session.execute(query1Stmt.bind());
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
            clNode = commandLine.getOptionValues("cn");
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
        options.addOption(Option.builder("cn").argName("cassandra-contact-points").hasArg().required(true).desc("Cassandra nodes ip").build());
        options.addOption(Option.builder("np").argName("cassandra-port").hasArg().desc("Cassandra port").build());
        return options;
    }
}
