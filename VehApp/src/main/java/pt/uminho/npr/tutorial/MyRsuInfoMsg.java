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

public class MyRsuInfoMsg extends V2xMessage {
    private final EncodedPayload payload;
    private final long timeStamp;
    private final String rsuName;
    private final GeoPoint rsuPosition;
    private final String additionalInfo;

    public MyRsuInfoMsg(MessageRouting routing, long time, String name, GeoPoint pos) {
        super(routing);
        this.timeStamp = time;
        this.rsuName = name;
        this.rsuPosition = pos;
        this.additionalInfo = "RSU ALERT: TRAFFIC MONITORING AND METEO CONDITIONS ACTIVE.";
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(rsuName);
            SerializationUtils.encodeGeoPoint(dos, rsuPosition);
            dos.writeUTF(additionalInfo);
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR SERIALIZING MYRSUINFOMSG", e);
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

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    @Override
    public String toString() {
        return "MYRSUINFOMSG{TIMESTAMP=" + timeStamp + ", RSUNAME='" + rsuName + "', RSUPOS=" + rsuPosition +
                ", ADDITIONALINFO='" + additionalInfo + "'}";
    }
}
