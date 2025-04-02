package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

/**
 * V2RACK MESSAGE - ACKNOWLEDGEMENT MESSAGE FOR UPSTREAM FLOW.
 * THIS MESSAGE CONFIRMS THE RECEPTION OF AN R2V MESSAGE AT THE FINAL DESTINATION
 * AND IS FORWARDED UPSTREAM TO THE RSU USING THE INVERTED FORWARDING CHECKLIST.
 */
public class V2RACK extends V2xMessage {
    private final String uniqueId;
    private final String originalMsgId;
    private final long timeStamp;
    private final long timestampLimit;
    private final String vehicleId;
    private final String rsuDestination;
    private final String nextHop;
    private final List<String> checklist; // THE INVERTED FORWARDING TRAIL FROM THE ORIGINAL R2V MESSAGE

    public V2RACK(MessageRouting routing, String uniqueId, String originalMsgId, long timeStamp, long timestampLimit,
                  String vehicleId, String rsuDestination, String nextHop, List<String> checklist) {
        super(routing);
        this.uniqueId = uniqueId;
        this.originalMsgId = originalMsgId;
        this.timeStamp = timeStamp;
        this.timestampLimit = timestampLimit;
        this.vehicleId = vehicleId;
        this.rsuDestination = rsuDestination;
        this.nextHop = nextHop;
        this.checklist = checklist;
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            // WRITE UNIQUE ID
            dos.writeUTF(uniqueId);
            // WRITE ORIGINAL MESSAGE ID FROM THE R2V MESSAGE
            dos.writeUTF(originalMsgId);
            // WRITE TIMESTAMP INFORMATION
            dos.writeLong(timeStamp);
            dos.writeLong(timestampLimit);
            // WRITE VEHICLE ID (ACK SENDER)
            dos.writeUTF(vehicleId);
            // WRITE RSU DESTINATION (TARGET RSU)
            dos.writeUTF(rsuDestination);
            // WRITE NEXT HOP FOR UPSTREAM FORWARDING
            dos.writeUTF(nextHop);
            // WRITE THE CHECKLIST SIZE AND ITEMS (INVERTED FORWARDING TRAIL)
            dos.writeInt(checklist.size());
            for (String item : checklist) {
                dos.writeUTF(item);
            }
            return new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING V2RACK", e);
        }
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public String getOriginalMsgId() {
        return originalMsgId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getTimestampLimit() {
        return timestampLimit;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public String getRsuDestination() {
        return rsuDestination;
    }

    public String getNextHop() {
        return nextHop;
    }

    public List<String> getChecklist() {
        return checklist;
    }

    @Override
    public String toString() {
        return "V2RACK MESSAGE - UNIQUEID: " + uniqueId +
               " | ORIGINAL_MSG_ID: " + originalMsgId +
               " | TIME: " + timeStamp +
               " | TIMESTAMP_LIMIT: " + timestampLimit +
               " | VEHICLE_ID: " + vehicleId +
               " | RSU_DESTINATION: " + rsuDestination +
               " | NEXT_HOP: " + nextHop +
               " | CHECKLIST: " + checklist;
    }
}