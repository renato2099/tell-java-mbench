package ch.ethz.mbench.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

/**
 * Contains information needed to manage client connections to server
 */
public class ClientSession {

    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    private SelectionKey selectionKey;
    private SocketChannel channel;

    public ClientSession(SelectionKey readKey, SocketChannel acceptedChannel) {
        this.selectionKey = readKey;
        this.channel = acceptedChannel;
        readBuffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
        writeBuffer = ByteBuffer.allocate(ServerCmd.CMD_SIZE);
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
            bytesRead = channel.read((ByteBuffer) getReadBuffer().clear());
        } catch (Throwable t) {
            // ignoring exception, if nothing is read we'll disconnect
        }

        if (bytesRead == -1) disconnect();
        if (bytesRead < 0) return null;

//        getReadBuffer().flip();
        ServerCmd serverCmd = ServerCmd.decodeCmd(getReadBuffer());
        getReadBuffer().clear();
        return serverCmd;
    }

    public void writeResponse(long respTime, long numRecords) {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(respTime).append("-");
            stringBuilder.append(numRecords);
            CharBuffer buffer = CharBuffer.wrap(stringBuilder.toString());
            while (buffer.hasRemaining())
                channel.write(Charset.defaultCharset().encode(buffer));
            buffer.clear();
//            getWriteBuffer().flip();
//            getWriteBuffer().clear();
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
