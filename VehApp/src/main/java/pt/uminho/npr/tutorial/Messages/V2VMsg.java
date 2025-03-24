package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.SerializationUtils;

public class V2VMsg extends V2xMessage {
    private final EncodedPayload payload;
    private final long timeStamp;
    private final String senderName;
    private final GeoPoint senderPos;

    public V2VMsg(MessageRouting routing, long time, String name, GeoPoint pos) {
        super(routing);
        this.timeStamp = time;
        this.senderName = name;
        this.senderPos = pos;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(senderName);
            SerializationUtils.encodeGeoPoint(dos, senderPos);
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch(IOException e) {
            throw new RuntimeException("ERROR SERIALIZING V2VMSG", e);
        }
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        return payload;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getSenderName() {
        return senderName;
    }

    public GeoPoint getSenderPos() {
        return senderPos;
    }

    @Override
    public String toString() {
        return "V2V MESSAGE - TIME: " + timeStamp + " | SENDER: " + senderName + " | POSITION: " + senderPos;
    }
}