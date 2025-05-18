package pt.uminho.npr.projeto.messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;

import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

public final class RsuAlertMessage extends V2xMessage {

    private final String          uniqueId;
    private final long            timestamp;
    private final String          originRsu;
    private final FogEventMessage alertEvent;

    public RsuAlertMessage(MessageRouting routing,
                           String uniqueId,
                           long timestamp,
                           String originRsu,
                           FogEventMessage alertEvent) {
        super(routing);
        this.uniqueId   = Objects.requireNonNull(uniqueId);
        this.timestamp  = timestamp;
        this.originRsu  = Objects.requireNonNull(originRsu);
        this.alertEvent = Objects.requireNonNull(alertEvent);
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {

            out.writeUTF(uniqueId);
            out.writeLong(timestamp);
            out.writeUTF(originRsu);

            byte[] evBytes = alertEvent.getPayload().getBytes();
            out.writeInt(evBytes.length);
            out.write(evBytes);

            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING RSU_ALERT_MESSAGE", e);
        }
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getOriginRsu() {
        return originRsu;
    }

    public FogEventMessage getAlertEvent() {
        return alertEvent;
    }

    @Override
    public String toString() {
        return "RSU_ALERT_MESSAGE : UNIQUE_ID: "   + uniqueId +
               " | ORIGIN_RSU: "                + originRsu +
               " | EVENT_ID: "                  + alertEvent.getUniqueId() +
               " | TIMESTAMP: "                 + timestamp;
    }
}