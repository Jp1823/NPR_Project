package pt.uminho.npr.tutorial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.SerializationUtils;

public class RsuFogMsg extends V2xMessage {
    private final EncodedPayload payload;
    private final long timeStamp;
    private final String rsuName;
    private final GeoPoint rsuPosition;
    private final String destNode;

    public RsuFogMsg(MessageRouting routing,
                     long time,
                     String rsuName,
                     GeoPoint rsuPos,
                     String dest) {
        super(routing);
        this.timeStamp = time;
        this.rsuName = rsuName;
        this.rsuPosition = rsuPos;
        this.destNode = dest;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(rsuName);
            SerializationUtils.encodeGeoPoint(dos, rsuPos);
            dos.writeUTF(destNode);
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR SERIALIZING RSUFOGMSG", e);
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

    public String getRsuName() {
        return rsuName;
    }

    public GeoPoint getRsuPosition() {
        return rsuPosition;
    }

    public String getDestNode() {
        return destNode;
    }

    @Override
    public String toString() {
        return "RSUFOGMSG{TIMESTAMP=" + timeStamp + ", RSUNAME='" + rsuName + "', POS=" + rsuPosition
               + ", DEST='" + destNode + "'}";
    }
}