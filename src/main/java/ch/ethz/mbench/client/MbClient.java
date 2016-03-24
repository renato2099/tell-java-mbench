package ch.ethz.mbench.client;

import ch.ethz.mbench.server.ServerCmd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

/**
 */
public class MbClient {

    public static void main(String[] args) throws IOException,
            InterruptedException {
        int port = 8713;
        SocketChannel channel = SocketChannel.open();

        // non blocking mode
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress("127.0.0.1", port));

        while (!channel.finishConnect()) {
             System.out.print(".");
        }
        ByteBuffer buffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);

        buffer.order(ByteOrder.nativeOrder());
        buffer.putLong(16);
        buffer.putInt(1);
        buffer.putInt(3); // scaling factor

//
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        while (true) {
            // see if any message has been received
            ByteBuffer bufferA = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
            int count = 0;
            String message = "";
            while ((count = channel.read(bufferA)) > 0) {
                // flip the buffer to start reading
                bufferA.flip();
                message += Charset.defaultCharset().decode(bufferA);
                System.out.println("Got from server");
            }

            if (message.length() > 0) {
                System.out.println(message);
//                buffer = ServerCmd.encodeCmd(ServerCmd.ServerCmdType.DISCONNECT, null);
//                while (buffer.hasRemaining()) {
//                    channel.write(buffer);
//                }
                break;
            }

        }
    }
}
