package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import pt.uminho.npr.tutorial.VehApp.VehicleGraph.NodeRecord;

/**
 * DEPTHNEIGHMSG CARRIES COMPLETE VEHICLEGRAPH STRUCTURE, INCLUDING ALL NODE METADATA.
 *
 * SERIALIZATION ORDER:
 *   1. MESSAGE_ID         (UTF)
 *   2. TIMESTAMP          (LONG)
 *   3. SENDER_ID          (UTF)
 *   4. TTL                (INT)
 *   5. MAP_SIZE           (INT)
 *   6. FOR EACH NODE ENTRY:
 *        a. VEHICLE_ID         (UTF)
 *        b. DISTANCE_TO_VEHICLE (DOUBLE)
 *        c. RSU_DISTANCE       (DOUBLE)
 *        d. RSU_REACHABLE      (BOOLEAN)
 *        e. NEIGHBOR_COUNT     (INT)
 *        f. NEIGHBOR_ID        (UTF) REPEATED
 *        g. NODE_TIMESTAMP     (LONG)
 */
public final class DepthNeighMsg extends V2xMessage {
    private final String messageId;
    private final long timeStamp;
    private final String senderId;
    private final int ttl;
    private final Map<String, NodeRecord> depthNeigh;

    /**
     * CONSTRUCTS DEPTHNEIGHMSG WITH DEEP COPY OF NODE RECORDS MAP.
     */
    public DepthNeighMsg(MessageRouting routing,
                         String messageId,
                         long timeStamp,
                         String senderId,
                         int ttl,
                         Map<String, NodeRecord> depthNeigh) {
        super(routing);
        this.messageId = Objects.requireNonNull(messageId, "MESSAGE_ID CANNOT BE NULL");
        this.timeStamp = timeStamp;
        this.senderId = Objects.requireNonNull(senderId, "SENDER_ID CANNOT BE NULL");
        this.ttl = ttl;

        // DEEP COPY AND MAKE IMMUTABLE
        Map<String, NodeRecord> copy = new HashMap<>();
        depthNeigh.forEach((k, v) -> {
            copy.put(
                Objects.requireNonNull(k, "VEHICLE_ID CANNOT BE NULL"),
                Objects.requireNonNull(v, "NODE_RECORD CANNOT BE NULL")
            );
        });
        this.depthNeigh = Collections.unmodifiableMap(copy);
    }

    public String getMessageId()               { return messageId; }
    public long getTimeStamp()                 { return timeStamp; }
    public String getSenderId()                { return senderId; }
    public int getTtl()                        { return ttl; }
    public Map<String, NodeRecord> getDepthNeigh() { return depthNeigh; }

    @Override
    @Nonnull
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos      = new DataOutputStream(baos)) {

            // WRITE HEADER
            dos.writeUTF(messageId);
            dos.writeLong(timeStamp);
            dos.writeUTF(senderId);
            dos.writeInt(ttl);

            // WRITE MAP SIZE
            dos.writeInt(depthNeigh.size());

            // WRITE EACH NODE RECORD
            for (Map.Entry<String, NodeRecord> entry : depthNeigh.entrySet()) {
                dos.writeUTF(entry.getKey());
                NodeRecord rec = entry.getValue();

                dos.writeDouble(rec.getDistanceToVehicle());
                dos.writeDouble(rec.getRsuDistance());
                dos.writeBoolean(rec.isRsuReachable());

                List<String> neighbors = rec.getNeighborList();
                dos.writeInt(neighbors.size());
                for (String nb : neighbors) {
                    dos.writeUTF(nb);
                }

                dos.writeLong(rec.getTimestamp());
            }

            byte[] payload = baos.toByteArray();
            return new EncodedPayload(payload, payload.length);

        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING DEPTHNEIGHMSG", e);
        }
    }

    @Override
    public String toString() {
        return String.format(
            "DEPTHNEIGHMSG - MESSAGE_ID: %s | TIMESTAMP: %d | SENDER: %s | TTL: %d | ENTRIES: %d",
            messageId, timeStamp, senderId, ttl, depthNeigh.size()
        );
    }
}
