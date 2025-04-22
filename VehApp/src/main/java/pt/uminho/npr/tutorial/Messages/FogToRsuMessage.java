package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

public final class FogToRsuMessage extends V2xMessage {

    private final String uniqueId;
    private final long timestamp;
    private final long expiryTimestamp;
    private final String messageType;
    private final String fogIdentifier;
    private final String rsuTarget;
    private final String vehicleTarget;
    private final String command;

    public FogToRsuMessage(MessageRouting routing,
                           String uniqueId,
                           long timestamp,
                           long expiryTimestamp,
                           String messageType,
                           String fogIdentifier,
                           String rsuTarget,
                           String vehicleTarget,
                           String command) {
        super(routing);
        this.uniqueId        = Objects.requireNonNull(uniqueId);
        this.timestamp       = timestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.messageType     = Objects.requireNonNull(messageType);
        this.fogIdentifier   = Objects.requireNonNull(fogIdentifier);
        this.rsuTarget       = Objects.requireNonNull(rsuTarget);
        this.vehicleTarget   = Objects.requireNonNull(vehicleTarget);
        this.command         = Objects.requireNonNull(command);
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {
            out.writeUTF(uniqueId);
            out.writeLong(timestamp);
            out.writeLong(expiryTimestamp);
            out.writeUTF(messageType);
            out.writeUTF(fogIdentifier);
            out.writeUTF(rsuTarget);
            out.writeUTF(vehicleTarget);
            out.writeUTF(command);
            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING FOG_TO_RSU_MESSAGE", e);
        }
    }

    public String getUniqueId()        { return uniqueId; }
    public long   getTimestamp()       { return timestamp; }
    public long   getExpiryTimestamp() { return expiryTimestamp; }
    public String getMessageType()     { return messageType; }
    public String getFogIdentifier()   { return fogIdentifier; }
    public String getRsuTarget()       { return rsuTarget; }
    public String getVehicleTarget()   { return vehicleTarget; }
    public String getCommand()         { return command; }

    @Override
    public String toString() {
        return "FOG_TO_RSU_MESSAGE : UNIQUE_ID: " + uniqueId +
               " | FOG_IDENTIFIER: " + fogIdentifier +
               " | RSU_TARGET: " + rsuTarget +
               " | VEHICLE_TARGET: " + vehicleTarget +
               " | COMMAND: " + command +
               " | TIMESTAMP: " + timestamp +
               " | EXPIRY_TIMESTAMP: " + expiryTimestamp;
    }
}