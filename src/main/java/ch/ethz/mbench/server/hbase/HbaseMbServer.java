package ch.ethz.mbench.server.hbase;

import ch.ethz.mbench.server.MbServer;
import ch.ethz.mbench.server.Tuple;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Hbase implementation of micro-benchmark
 */
public class HbaseMbServer extends MbServer {

    public static final String CONTAINER = "mbench";
    public static final String TABLE_NAME = "maintable";
    public static Logger Log = Logger.getLogger(HbaseMbServer.class);


    // class attributes
    private static String zook;
    private static String zkPort;
    private static String hMaster;
    private static String hPort;

    public static void main(String args[]) {
        HbaseMbServer mserver = new HbaseMbServer();
        mserver.parseCmdLine(args);
        try {
            mserver.run();
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Connection createConnection() {
        return new HbaseConnection();
    }

    /**
     * Creates Hbase connection
     */
    public static class HbaseConnection implements Connection {

        private static HBaseAdmin admin;

        HbaseConnection() {
            try {
                admin = new HBaseAdmin(createHBaseConf());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        @Override
        public Transaction startTx() {
            return new HbaseTransaction();
        }

        @Override
        public void createSchema(int nCols) {
            try {
                HTableDescriptor desc = new HTableDescriptor(CONTAINER);
                HColumnDescriptor tabName = new HColumnDescriptor(TABLE_NAME);
                // trying to keep it in memory
                tabName.setInMemory(true);
                desc.addFamily(tabName);
                if (!admin.tableExists(CONTAINER))
                    admin.createTable(desc);
                Log.warn(String.format("Table %s created.", TABLE_NAME));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Handles operations in Hbase
     */
    public static class HbaseTransaction implements Transaction {

        private HTable hTable = null;

        HbaseTransaction() {
            try {
                hTable = new HTable(createHBaseConf(), CONTAINER);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void insert(Long key, Tuple value) {
            try {
                Put put = new Put(Bytes.toBytes(key));
                for (int i = 0; i < value.getNumFields(); i++) {
                    if (value.getFieldValues()[i] != null)
                        switch (i) {
                            case 0:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((double) value.getFieldValues()[i]));
                                break;
                            case 1:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((int) value.getFieldValues()[i]));
                                break;
                            case 2:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((int) value.getFieldValues()[i]));
                                break;
                            case 3:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((short) value.getFieldValues()[i]));
                                break;
                            case 4:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((short) value.getFieldValues()[i]));
                                break;
                            case 5:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((long) value.getFieldValues()[i]));
                                break;
                            case 6:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((long) value.getFieldValues()[i]));
                                break;
                            case 7:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((double) value.getFieldValues()[i]));
                                break;
                            case 8:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((String) value.getFieldValues()[i]));
                                break;
                            case 9:
                                put.addColumn(Bytes.toBytes(TABLE_NAME),
                                        Bytes.toBytes(value.getFieldNames()[i]),
                                        Bytes.toBytes((String) value.getFieldValues()[i]));
                                break;
                            default:
                                throw new RuntimeException("Number of columns not supported");
                        }
                }
                hTable.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void commit() {
            try {
                // commit batch
                // hTable.flushCommits(); close calls flushcommits
                hTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void update(Long key, Tuple value) {
            insert(key, value);
        }

        @Override
        public void remove(Long key) {
            try {
                Delete delete = new Delete(Bytes.toBytes(key));
                hTable.delete(delete);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void get(Long key) {
            try {
                Get get = new Get(Bytes.toBytes(key));
                byte[] row = hTable.get(get).getRow();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static Configuration createHBaseConf() throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zook);
        hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort);
        hbaseConf.set("hbase.master", hMaster.concat(":").concat(hPort));
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        return hbaseConf;
    }

    @Override
    public void parseCmdLine(String args[]) {
        super.parseCmdLine(args);
        CommandLine commandLine;
        CommandLineParser parser = new DefaultParser();
        Options options = getCmdLineOptions();
        try {
            commandLine = parser.parse(options, args);
            hMaster = commandLine.getOptionValue("hm");
            hPort = commandLine.getOptionValue("hp", "60000");
            zook = commandLine.getOptionValue("zm");
            zkPort = commandLine.getOptionValue("zp", "2181");
        } catch (ParseException exception) {
            Log.error("Parse error: ");
            new HelpFormatter().printHelp("mbench-server", options);
            System.exit(0);
        }
    }

    @Override
    public Options getCmdLineOptions() {
        Options options = super.getCmdLineOptions();
        options.addOption(Option.builder("hm").argName("hbase-master").hasArg().required(true).desc("HBase master server ip").build());
        options.addOption(Option.builder("hp").argName("hbase-port").hasArg().desc("HBase port").build());
        options.addOption(Option.builder("zm").argName("zk-master").hasArg().required(true).desc("Zookeeper master server ip").build());
        options.addOption(Option.builder("zp").argName("zk-port").hasArg().desc("Zookeeper port").build());
        return options;
    }
}
