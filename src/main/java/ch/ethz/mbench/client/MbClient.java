package ch.ethz.mbench.client;

import ch.ethz.mbench.server.ServerCmd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

import static ch.ethz.mbench.server.ServerCmd.ServerCmdType.BATCH_OP;
import static ch.ethz.mbench.server.ServerCmd.ServerCmdType.CREATE_SCHEMA;
import static ch.ethz.mbench.server.ServerCmd.ServerCmdType.POPULATE;

/**
 */
public class MbClient {


    public static void main(String[] args) throws IOException,
            InterruptedException {
        int port = 8713;
        SocketChannel channel = SocketChannel.open();
        Queue<ServerCmd> cmdQueue = new LinkedList<>();

        // non blocking mode
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress("127.0.0.1", port));

        while (!channel.finishConnect()) {
            System.out.print(".");
        }
//        ByteBuffer buffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
//        buffer.order(ByteOrder.nativeOrder());
//        buffer.putLong(16);
//        buffer.putInt(1);
//        buffer.putInt(3); // scaling factor
//        buffer.flip();

        ServerCmd createCmd = new ServerCmd(CREATE_SCHEMA, new Object[]{CREATE_SCHEMA.getVal() + 1, 3});
        cmdQueue.add(createCmd);
        cmdQueue.add(new ServerCmd(POPULATE, new Object[]{POPULATE.getVal() + 1, 0L, 10L}));
//        cmdQueue.add(new ServerCmd(BATCH_OP, new Object[]{0L, 10L}));

        while (!cmdQueue.isEmpty()) {
            ServerCmd cmd = cmdQueue.poll();
            ByteBuffer byteBuffer = ServerCmd.encodeServerCmd(cmd);

            // send command
            while (byteBuffer.hasRemaining()) {
                channel.write(byteBuffer);
            }
            // wait for response
            if (!waitResponse(cmd, channel)) {
                System.out.println("Error processing command " + cmd.getType().toString());
                System.exit(1);
            } else {
                System.out.println(cmd.getType().toString() + " successfully executed!");
            }

        }
    }

    private static boolean waitResponse(ServerCmd cmd, SocketChannel channel) throws IOException {
        boolean result = false;
        boolean read = false;
        while (true) {
            // see if any message has been received
            ByteBuffer bufferA = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
            bufferA.clear();
            bufferA.order(ByteOrder.nativeOrder());
            int count = 0;
            String message = "";
            while ((count = channel.read(bufferA)) > 0) {
                // flip the buffer to start reading
                bufferA.flip();
                System.out.println("Server response size:" + bufferA.getLong());
                System.out.println("Bytes read from server:" + count);
                result = bufferA.get() == 1 ? true : false;
                switch (cmd.getType()) {
                    case CREATE_SCHEMA:
                        break;
                    case POPULATE:
                        int strSz = bufferA.getInt();
                        StringBuilder sb = new StringBuilder();
                        byte byteStr[] = new byte[strSz];
                        bufferA.get(byteStr, 0, strSz);
                        System.out.println(new String(byteStr, StandardCharsets.UTF_8));
                        break;
                    case BATCH_OP:
                        break;
                    case Q1:
                        break;
                    case Q2:
                        break;
                    case Q3:
                        break;
                    case DISCONNECT:
                        break;
                }
                read = true;
//                message += Charset.defaultCharset().decode(bufferA);
//                System.out.println("Got from server");
            }

            if (read)
                break;
//            if (message.length() > 0) {
//                System.out.println(message);
//                buffer = ServerCmd.encodeCmd(ServerCmd.ServerCmdType.DISCONNECT, null);
//                while (buffer.hasRemaining()) {
//                    channel.write(buffer);
//                }
//                break;
//            }
        }
        return result;
    }
}
