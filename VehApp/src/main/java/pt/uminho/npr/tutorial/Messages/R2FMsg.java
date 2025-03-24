package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

public class R2FMsg extends V2xMessage {
    private final long timeStamp;
    private final String rsuName;
    private final String receiver;
    private final V2xMessage innerMsg;

    public R2FMsg(MessageRouting routing, long time, String rsuName, String receiver, V2xMessage innerMsg) {
        super(routing);
        this.timeStamp = time;
        this.rsuName = rsuName;
        this.receiver = receiver;
        this.innerMsg = innerMsg;
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(rsuName);
            dos.writeUTF(receiver);
            EncodedPayload innerPayload = innerMsg.getPayload();
            byte[] innerBytes = innerPayload.getBytes();
            int innerLength = innerBytes.length;
            dos.writeInt(innerLength);
            dos.write(innerBytes, 0, innerLength);
            return new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING R2FMSG", e);
        }
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getRsuName() {
        return rsuName;
    }

    public String getReceiver() {
        return receiver;
    }

    public V2xMessage getInnerMsg() {
        return innerMsg;
    }

    @Override
    public String toString() {
        return "R2F MESSAGE - TIME: " + timeStamp +
               " | RSU: " + rsuName +
               " | RECEIVER: " + receiver +
               " | INNER MSG: " + innerMsg;
    }
}