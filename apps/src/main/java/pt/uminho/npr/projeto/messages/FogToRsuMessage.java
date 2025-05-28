package pt.uminho.npr.projeto.messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

public final class FogToRsuMessage extends V2xMessage {

    private final long timestamp;
    private final long expiryTimestamp;
    private final String messageType;
    private final String fogIdentifier;
    private final String vehicleTarget;
    private final EventMessage commandEvent;

    public FogToRsuMessage(MessageRouting routing,
                           long timestamp,
                           long expiryTimestamp,
                           String messageType,
                           String fogIdentifier,
                           String vehicleTarget,
                           EventMessage commandEvent) {
        super(routing);
        this.timestamp       = timestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.messageType     = Objects.requireNonNull(messageType);
        this.fogIdentifier   = Objects.requireNonNull(fogIdentifier);
        this.vehicleTarget   = Objects.requireNonNull(vehicleTarget);
        this.commandEvent    = Objects.requireNonNull(commandEvent);
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf)) {

            out.writeLong(timestamp);
            out.writeLong(expiryTimestamp);
            out.writeUTF(messageType);
            out.writeUTF(fogIdentifier);
            out.writeUTF(vehicleTarget);

            byte[] evBytes = commandEvent.getPayload().getBytes();
            out.writeInt(evBytes.length);
            out.write(evBytes);

            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING FOG_TO_RSU_MESSAGE", e);
        }
    }

    public long   getTimestamp()            { return timestamp; }
    public long   getExpiryTimestamp()      { return expiryTimestamp; }
    public String getMessageType()          { return messageType; }
    public String getFogIdentifier()        { return fogIdentifier; }
    public String getVehicleTarget()        { return vehicleTarget; }
    public EventMessage getCommandEvent(){ return commandEvent; }

    @Override
    public String toString() {
        return "FOG_TO_RSU_MESSAGE :" +
               " | FOG_IDENTIFIER: " + fogIdentifier +
               " | VEHICLE_TARGET: " + vehicleTarget +
               " | MESSAGE_TYPE: " + messageType +
               " | TIMESTAMP: " + timestamp +
               " | EXPIRY_TIMESTAMP: " + expiryTimestamp +
               " | EVENT_PAYLOAD_SIZE: " + commandEvent.getPayload().getBytes().length;
    }
}