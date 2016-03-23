package ch.ethz.mbench.server;

import java.nio.ByteBuffer;

/**
 * Handles clients' encoded commands.
 */
public class ServerCmd {

    public enum ServerCmdType {
        POPULATE(0), BATCH_OP (1), GET(2), DISCONNECT(3), Q1(4), Q2(5), Q3(6);
        short val;
        ServerCmdType(int v) {
            this.val = (short)v;
        }
    }

    private ServerCmdType type;
    private Object args[];
    public static int CMD_SIZE = 25;

    public ServerCmd() {
    }

    public static ByteBuffer encodeCmd(ServerCmdType cmdType, Object...args) {
        ByteBuffer bb = ByteBuffer.allocate(CMD_SIZE);
        // command
        bb.asShortBuffer().put(cmdType.val);
        int pos = 4;
        switch(cmdType) {
            case POPULATE:
                // lower key
                bb.position(pos);
                bb.asLongBuffer().put((Long)args[0]);
                pos += 8;
                bb.position(pos);
                // higher key
                bb.asLongBuffer().put((Long)args[1]);
//                bb.flip();
                break;
            case BATCH_OP:
                break;
            case GET:
                break;
            case DISCONNECT:
                break;
        }
        return bb;
    }

    public static ServerCmd decodeCmd(ByteBuffer bb) {
        ServerCmd scm = new ServerCmd();
        int pos = 0;
        scm.type = ServerCmd.ServerCmdType.values()[bb.getShort(0)];
        pos += 4;
        switch (scm.type) {
            case POPULATE:
                scm.args = new Object[2];
                bb.position(pos);
                scm.args[0] = bb.getLong(pos);
                pos += 8;
                bb.position(pos);
                scm.args[1] = bb.getLong(pos);
                System.out.println("cmd =>" + scm.args[0] + " "+scm.args[1]);
                break;
            case BATCH_OP:
                break;
            case GET:
                break;
            case DISCONNECT:
                break;
        }
        return scm;
    }



    public ServerCmd(ServerCmdType t, String a[]) {
        this.type = t;
        this.args = a;
    }
//    public static ServerCmd decode(ByteBuffer cmd) {
//        String[] strCmd = Charset.defaultCharset().decode(cmd).toString().split(" ");
//        System.out.println(String.format("lkey:%s - hkey:%s", strCmd[1], strCmd[2]));
//        return new ServerCmd(ServerCmdType.POPULATE, Arrays.copyOfRange(strCmd, 1, strCmd.length));
//    }
    public ServerCmdType getType() {
        return type;
    }
    public Object[] getArgs() {
        return args;
    }
}
