package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;
import javax.annotation.Nonnull;

import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.SerializationUtils;

public final class FogEventMessage extends V2xMessage {

    private final String               uniqueId;
    private final long                 timestamp;
    private final long                 expiryTimestamp;
    private final String               fogSource;
    private final String               eventType;
    private final GeoPoint             location;
    private final Map<String,String>   parameters;

    public FogEventMessage(MessageRouting routing,
                           String uniqueId,
                           long timestamp,
                           long expiryTimestamp,
                           String fogSource,
                           String eventType,
                           GeoPoint location,
                           Map<String,String> parameters) {
        super(routing);
        this.uniqueId        = Objects.requireNonNull(uniqueId);
        this.timestamp       = timestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.fogSource       = Objects.requireNonNull(fogSource);
        this.eventType       = Objects.requireNonNull(eventType);
        this.location        = Objects.requireNonNull(location);
        this.parameters      = Collections.unmodifiableMap(new java.util.HashMap<>(Objects.requireNonNull(parameters)));
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {

            out.writeUTF(uniqueId);
            out.writeLong(timestamp);
            out.writeLong(expiryTimestamp);
            out.writeUTF(fogSource);
            out.writeUTF(eventType);
            // serializar localização
            SerializationUtils.encodeGeoPoint(out, location);
            // serializar parâmetros
            out.writeInt(parameters.size());
            for (Map.Entry<String,String> e : parameters.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }

            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING FOG_EVENT_MESSAGE", e);
        }
    }

    public String getUniqueId()             { return uniqueId; }
    public long   getTimestamp()            { return timestamp; }
    public long   getExpiryTimestamp()      { return expiryTimestamp; }
    public String getFogSource()            { return fogSource; }
    public String getEventType()            { return eventType; }
    public GeoPoint getLocation()           { return location; }
    public Map<String,String> getParameters() { return parameters; }

    @Override
    public String toString() {
        return "FOG_EVENT_MESSAGE : UNIQUE_ID: "     + uniqueId +
               " | FOG_SOURCE: "       + fogSource +
               " | EVENT_TYPE: "       + eventType +
               " | LOCATION: "         + location +
               " | PARAM_COUNT: "      + parameters.size() +
               " | TIMESTAMP: "        + timestamp +
               " | EXPIRY_TIMESTAMP: " + expiryTimestamp;
    }
}