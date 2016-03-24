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

    private long getResultBufferSize(Object[] results) throws IOException {
        long result = 8;    // 8 bytes for encoding the total size
        for (Object o: results) {
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

    public void writeResponse(Object[] results) {
        try {
            getWriteBuffer().clear();
            getWriteBuffer().putLong(getResultBufferSize(results));
            for (Object o: results) {
                if (o instanceof Boolean) {
                    Boolean b = (Boolean) o;
                    getWriteBuffer().put(b ? oneByte : zeroByte);
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
                    throw new RuntimeException("no known object serialization method for object " + o.toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
