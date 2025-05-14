package pt.uminho.npr.projeto.messages;

import java.io.*;
import java.util.*;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.*;

public final class VehicleToRsuACK extends V2xMessage {

    private final String uniqueId;
    private final String originalMessageId;
    private final long timestamp;
    private final long expiryTimestamp;
    private final String vehicleIdentifier;
    private final String rsuDestination;
    private final String nextHop;
    private final List<String> checklist;

    public VehicleToRsuACK(MessageRouting routing,
                           String uniqueId,
                           String originalMessageId,
                           long timestamp,
                           long expiryTimestamp,
                           String vehicleIdentifier,
                           String rsuDestination,
                           String nextHop,
                           List<String> checklist) {
        super(routing);
        this.uniqueId          = Objects.requireNonNull(uniqueId);
        this.originalMessageId = Objects.requireNonNull(originalMessageId);
        this.timestamp         = timestamp;
        this.expiryTimestamp   = expiryTimestamp;
        this.vehicleIdentifier = Objects.requireNonNull(vehicleIdentifier);
        this.rsuDestination    = Objects.requireNonNull(rsuDestination);
        this.nextHop           = Objects.requireNonNull(nextHop);
        this.checklist         = List.copyOf(Objects.requireNonNull(checklist));
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {
            out.writeUTF(uniqueId);
            out.writeUTF(originalMessageId);
            out.writeLong(timestamp);
            out.writeLong(expiryTimestamp);
            out.writeUTF(vehicleIdentifier);
            out.writeUTF(rsuDestination);
            out.writeUTF(nextHop);
            out.writeInt(checklist.size());
            for (String item : checklist) {
                out.writeUTF(item);
            }
            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING VEHICLE_TO_RSU_ACK", e);
        }
    }

    public String getUniqueId()          { return uniqueId; }
    public String getOriginalMessageId() { return originalMessageId; }
    public long   getTimestamp()         { return timestamp; }
    public long   getExpiryTimestamp()   { return expiryTimestamp; }
    public String getVehicleIdentifier() { return vehicleIdentifier; }
    public String getRsuDestination()    { return rsuDestination; }
    public String getNextHop()           { return nextHop; }
    public List<String> getChecklist()   { return checklist; }

    @Override
    public String toString() {
        return "VEHICLE_TO_RSU_ACK : UNIQUE_ID: " + uniqueId +
               " | ORIGINAL_MESSAGE_ID: " + originalMessageId +
               " | VEHICLE_IDENTIFIER: " + vehicleIdentifier +
               " | NEXT_HOP: " + nextHop +
               " | RSU_DESTINATION: " + rsuDestination +
               " | CHECKLIST_SIZE: " + checklist.size() +
               " | TIMESTAMP: " + timestamp +
               " | EXPIRY_TIMESTAMP: " + expiryTimestamp;
    }
}
