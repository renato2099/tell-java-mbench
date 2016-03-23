package ch.ethz.mbench.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.lang.instrument.Instrumentation;

/**
 * Contains information needed to manage client connections to server
 */
public class ClientSession {

    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    private SelectionKey selectionKey;
    private SocketChannel channel;

    private static Instrumentation instrumentation;
    private static byte zeroByte = 0, oneByte = 1;

    public ClientSession(SelectionKey readKey, SocketChannel acceptedChannel) {
        this.selectionKey = readKey;
        this.channel = acceptedChannel;
        readBuffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
        readBuffer.order(ByteOrder.nativeOrder());
        writeBuffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
        writeBuffer.order(ByteOrder.nativeOrder());
    }

    public void disconnect() {
        MbServer.clientMap.remove(selectionKey);
        try {
            if (selectionKey != null) selectionKey.cancel();
            if (channel == null) return;
            System.out.println("Disconnecting client");
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ServerCmd readCmd() {
        int bytesRead = -1;

        try {
            bytesRead = channel.read((ByteBuffer) readBuffer.clear());
        } catch (Throwable t) {
            // ignoring exception, if nothing is read we'll disconnect
        }
        readBuffer.flip();
        if (bytesRead == -1) disconnect();
        if (bytesRead < 0) return null;
        ServerCmd serverCmd = ServerCmd.decodeCmd(getReadBuffer());
        getReadBuffer().clear();
        return serverCmd;
    }

    public void writeResponse(Object[] results) {
//        try {
            long totalSize = 0;
            for (Object o: results) {
                totalSize += instrumentation.getObjectSize(o);
            }
            getWriteBuffer().clear();
            getWriteBuffer().putLong(totalSize);
            for (Object o: results) {
                if (o instanceof Boolean) {
                    Boolean b = (Boolean) o;
                    getWriteBuffer().put(b ? oneByte : zeroByte);
                } else if (o instanceof Short) {
                    //TODO: continue...
                }
            }

//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void setChannel(SocketChannel c) {
        this.channel = c;
    }

    public SocketChannel getChannel() {
        return this.channel;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }
}
