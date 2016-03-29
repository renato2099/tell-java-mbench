package ch.ethz.mbench.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Handles clients' encoded commands.
 */
public class ServerCmd {

    public enum ServerCmdType {
        CREATE_SCHEMA(0), POPULATE(1), BATCH_OP(2), Q1(3), Q2(4), Q3(5), DISCONNECT(6);
        short val;

        ServerCmdType(int v) {
            this.val = (short) v;
        }

        public static ServerCmdType fromInt(int v) {
            return ServerCmdType.values()[v - 1]; // CAUTION: this only works because numbers are consecutive
        }
        public int getVal() {
            return val + 1;
        }
    }

    private ServerCmdType type;
    private Object args[];
    public static int CMD_SIZE = 128;

    public ServerCmd(ServerCmdType t, Object a[]) {
        this.type = t;
        this.args = a;
    }

    public static long getResultBufferSize(Object... args) throws IOException {
        long result = 8;    // 8 bytes for encoding the total size
        for (Object o : args) {
            if (o instanceof Boolean || o instanceof Byte) {
                result += 1;
            } else if (o instanceof Short) {
                result += 2;
            } else if (o instanceof Integer || o instanceof Float) {
                result += 4;
            } else if (o instanceof Long || o instanceof Double) {
                result += 8;
            } else if (o instanceof String) {
                result += 4;    // encode string length
                result += (((String) o).getBytes("UTF-8").length);
            } else {
                throw new RuntimeException("no known object serialization method for object " + o.toString());
            }
        }
        return result;
    }

    public static ServerCmd decodeCmd(ByteBuffer bb) {
        Object[] resultArr = null;
        bb.getLong();   // total buffer size, currently not used
        ServerCmdType resultType = ServerCmdType.fromInt(bb.getInt());
        switch (resultType) {
            case CREATE_SCHEMA:
                resultArr = new Object[]{bb.getInt()};  // scaling factor
                break;
            case POPULATE:
                resultArr = new Object[]{bb.getLong(), bb.getLong()};   // startKey (included), endKey (excluded) to populate
                //System.out.println("cmd =>" + scm.args[0] + " "+scm.args[1]);
                break;
            case BATCH_OP:
                resultArr = new Object[]{
                        bb.getInt(),    // 0: number of Ops
                        bb.getDouble(), // 1: insertion probability
                        bb.getDouble(), // 2: deletion probability
                        bb.getDouble(), // 3: update probability
                        bb.getInt(),    // 4: client-id
                        bb.getLong(),   // 5: number of clients
                        bb.getLong(),   // 6: base insert key
                        bb.getLong(),   // 7: base delete key
                };
                break;
            case Q1:
            case Q2:
            case Q3:
                resultArr = new Object[]{};
                break;
            case DISCONNECT:
                break;
        }
        return new ServerCmd(resultType, resultArr);
    }

    public static ByteBuffer encodeServerCmd(ServerCmd cmd) throws IOException {
        // cmdSize = cmdType + cmdArgs
        long cmdSz = getResultBufferSize(cmd.args);
        ByteBuffer bb = ByteBuffer.allocate((int)cmdSz);
        bb.order(ByteOrder.nativeOrder());
        bb.clear();
        bb.putLong(cmdSz);
        // setting type offset
        for (Object o: cmd.args) {
            if (o instanceof Boolean) {
                Boolean b = (Boolean) o;
                bb.put((byte)(b ? 1 : 0));
            } else if (o instanceof Byte) {
                bb.put((Byte) o);
            } else if (o instanceof Short) {
                bb.putShort((Short) o);
            } else if (o instanceof Integer) {
                bb.putInt((Integer) o);
            } else if (o instanceof Float) {
                bb.putFloat((Float) o);
            } else if (o instanceof Long) {
                bb.putLong((Long) o);
            } else if (o instanceof Double) {
                bb.putDouble((Double) o);
            } else if (o instanceof String) {
                byte[] utf8Arr = ((String) o).getBytes("UTF-8");
                bb.putInt(utf8Arr.length);
                bb.put(utf8Arr);
            } else {
                throw new RuntimeException("Not able to serialize type " + o.getClass().getSimpleName());
            }
        }
        bb.flip();
        return bb;
    }

    public ServerCmdType getType() {
        return type;
    }

    public Object[] getArgs() {
        return args;
    }

}
