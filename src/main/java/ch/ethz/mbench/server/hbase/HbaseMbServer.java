package ch.ethz.mbench.server.hbase;

import ch.ethz.mbench.server.MbServer;
import ch.ethz.mbench.server.Tuple;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
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
        
         hBaseConf = createHBaseConf();
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

    private static  Configuration hBaseConf ;

    public static Configuration createHBaseConf() {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zook);
        hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort);
        hbaseConf.set("hbase.master", hMaster.concat(":").concat(hPort));
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");
        return hbaseConf;
    }

    /**
     * Creates Hbase connection
     */
    public static class HbaseConnection implements Connection {
        private final AggregationClient aggrClient = new AggregationClient(hBaseConf);

        @Override
        public Transaction startTx() {
            try {
                return new HbaseTransaction(aggrClient);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public void createSchema(int nCols) {
            try {
                HBaseAdmin admin = new HBaseAdmin(createHBaseConf());
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
        private final HTable hTable;
        private final AggregationClient aggrClient;

        HbaseTransaction(AggregationClient aggrClient) throws IOException{
            this.aggrClient = aggrClient;
            hTable = new HTable(hBaseConf, CONTAINER);
        }

        @Override
        public boolean insert(Long key, Tuple value) {
            boolean result = false;
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
                result = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean commit() {
            boolean result = false;
            try {
                // commit batch
                // hTable.flushCommits(); close calls flushcommits
                hTable.close();
                result = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean update(Long key, Tuple value) {
            return insert(key, value);
        }

        @Override
        public boolean remove(Long key) {
            boolean result = false;
            try {
                Delete delete = new Delete(Bytes.toBytes(key));
                hTable.delete(delete);
                result = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean get(Long key) {
            boolean result = false;
            try {
                Get get = new Get(Bytes.toBytes(key));
                byte[] row = hTable.get(get).getRow();
                result = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public long query1() {
            try {
                Scan scan = new Scan();
                scan.addColumn(TABLE_NAME.getBytes(), "A0".getBytes());
                Double maxVal = aggrClient.max(hTable.getName(), null, scan);
                System.out.println("MaxVal:" + maxVal);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
            throw new RuntimeException("Query not supported!");
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
