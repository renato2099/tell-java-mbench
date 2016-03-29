package ch.ethz.mbench.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Contains information needed to manage client connections to server
 */
public class ClientSession {

    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    private SelectionKey selectionKey;
    private SocketChannel channel;
    private long clientId;

    public ClientSession(SelectionKey readKey, SocketChannel acceptedChannel, long cId) {
        this.selectionKey = readKey;
        this.channel = acceptedChannel;
        this.clientId = cId;
        readBuffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
        readBuffer.order(ByteOrder.nativeOrder());
        writeBuffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
        writeBuffer.order(ByteOrder.nativeOrder());
    }

    public void disconnect() {
        MbServer.clientMap.remove(selectionKey);
        MbServer.clientIds --;
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
        getReadBuffer().clear();

        try {
            bytesRead = channel.read(getReadBuffer());
        } catch (Throwable t) {
            // ignoring exception, if nothing is read we'll disconnect
        }
        getReadBuffer().flip();
        if (bytesRead == -1) disconnect();
        if (bytesRead < 0) return null;
        ServerCmd serverCmd = ServerCmd.decodeCmd(getReadBuffer());
        getReadBuffer().clear();
        return serverCmd;
    }

    public void writeResponse(Object[] results) {
        try {
            getWriteBuffer().clear();
            getWriteBuffer().putLong(ServerCmd.getResultBufferSize(results));
            for (Object o: results) {
                if (o instanceof Boolean) {
                    Boolean b = (Boolean) o;
                    getWriteBuffer().put((byte)(b ? 1 : 0));
                } else if (o instanceof Byte) {
                    getWriteBuffer().put((Byte) o);
                } else if (o instanceof Short) {
                    getWriteBuffer().putShort((Short) o);
                } else if (o instanceof Integer) {
                    getWriteBuffer().putInt((Integer) o);
                } else if (o instanceof Float) {
                    getWriteBuffer().putFloat((Float) o);
                } else if (o instanceof Long) {
                    getWriteBuffer().putLong((Long) o);
                } else if (o instanceof Double) {
                    getWriteBuffer().putDouble((Double) o);
                } else if (o instanceof String) {
                    byte[] utf8Arr = ((String) o).getBytes("UTF-8");
                    getWriteBuffer().putInt(utf8Arr.length);
                    getWriteBuffer().put(utf8Arr);
                } else {
                    throw new RuntimeException("Not able to serialize type " + o.getClass().getSimpleName());
                }
            }
            // write it to the channel
            getWriteBuffer().flip();
            while(getWriteBuffer().hasRemaining())
                getChannel().write(getWriteBuffer());
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public long getClientId() {
        return clientId;
    }
}
