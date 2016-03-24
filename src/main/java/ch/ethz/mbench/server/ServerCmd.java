package ch.ethz.mbench.server;

import java.nio.ByteBuffer;

/**
 * Handles clients' encoded commands.
 */
public class ServerCmd {

    public enum ServerCmdType {
        CREATE_SCHEMA(1), POPULATE(2), BATCH_OP(3), Q1(4), Q2(5), Q3(6), DISCONNECT(7);
        short val;
        ServerCmdType(int v) {
            this.val = (short)v;
        }
        public static ServerCmdType fromInt (int v) {
            return ServerCmdType.values()[v-1]; // CAUTION: this only works because numbers are consecutive
        }
    }

    public ServerCmd(ServerCmdType t, Object a[]) {
        this.type = t;
        this.args = a;
    }

    public ServerCmdType getType() {
        return type;
    }

    public Object[] getArgs() {
        return args;
    }

    private ServerCmdType type;
    private Object args[];
    public static int CMD_SIZE = 25;

    public static ServerCmd decodeCmd(ByteBuffer bb) {
        Object[] resultArr = null;
        bb.getLong();   // total buffer size, currently not used
        ServerCmdType resultType = ServerCmdType.fromInt(bb.getInt());
        switch (resultType) {
            case CREATE_SCHEMA:
                resultArr = new Object[] {bb.getInt()};  // scaling factor
                break;
            case POPULATE:
                resultArr = new Object[] {bb.getLong(), bb.getLong()};   // startKey (included), endKey (excluded) to populate
                //System.out.println("cmd =>" + scm.args[0] + " "+scm.args[1]);
                break;
            case BATCH_OP:
                resultArr = new Object[] {
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
}
