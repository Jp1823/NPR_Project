package pt.uminho.npr.tutorial.Messages;

import java.io.*;
import java.util.*;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.*;

public final class RsuToVehicleMessage extends V2xMessage {

    private final String uniqueId;
    private final long timestamp;
    private final long expiryTimestamp;
    private final String rsuSource;
    private final String vehicleTarget;
    private final String nextHop;
    private final String command;
    private final List<String> forwardingTrail;

    public RsuToVehicleMessage(MessageRouting routing,
                               String uniqueId,
                               long timestamp,
                               long expiryTimestamp,
                               String rsuSource,
                               String vehicleTarget,
                               String nextHop,
                               String command,
                               List<String> forwardingTrail) {
        super(routing);
        this.uniqueId        = Objects.requireNonNull(uniqueId);
        this.timestamp       = timestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.rsuSource       = Objects.requireNonNull(rsuSource);
        this.vehicleTarget   = Objects.requireNonNull(vehicleTarget);
        this.nextHop         = Objects.requireNonNull(nextHop);
        this.command         = Objects.requireNonNull(command);
        this.forwardingTrail = List.copyOf(Objects.requireNonNull(forwardingTrail));
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {
            out.writeUTF(uniqueId);
            out.writeLong(timestamp);
            out.writeLong(expiryTimestamp);
            out.writeUTF(rsuSource);
            out.writeUTF(vehicleTarget);
            out.writeUTF(nextHop);
            out.writeUTF(command);
            out.writeInt(forwardingTrail.size());
            for (String hop : forwardingTrail) {
                out.writeUTF(hop);
            }
            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING RSU_TO_VEHICLE_MESSAGE", e);
        }
    }

    public String getUniqueId()        { return uniqueId; }
    public long   getTimestamp()       { return timestamp; }
    public long   getExpiryTimestamp() { return expiryTimestamp; }
    public String getRsuSource()       { return rsuSource; }
    public String getVehicleTarget()   { return vehicleTarget; }
    public String getNextHop()         { return nextHop; }
    public String getCommand()         { return command; }
    public List<String> getForwardingTrail() { return forwardingTrail; }

    @Override
    public String toString() {
        return "RSU_TO_VEHICLE_MESSAGE : UNIQUE_ID: " + uniqueId +
               " | RSU_SOURCE: " + rsuSource +
               " | VEHICLE_TARGET: " + vehicleTarget +
               " | NEXT_HOP: " + nextHop +
               " | COMMAND: " + command +
               " | FORWARDING_TRAIL_SIZE: " + forwardingTrail.size() +
               " | TIMESTAMP: " + timestamp +
               " | EXPIRY_TIMESTAMP: " + expiryTimestamp;
    }
}