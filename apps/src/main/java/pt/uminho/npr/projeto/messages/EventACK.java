package pt.uminho.npr.projeto.messages;

import java.io.*;
import java.util.*;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.*;

public final class EventACK extends V2xMessage {

    private final long timestamp;
    private final long expiryTimestamp;
    private final List<String> checklist;

    public EventACK(MessageRouting routing,
                           int ackId,
                           long timestamp,
                           long expiryTimestamp,
                           List<String> checklist) {
        super(routing, ackId);
        this.timestamp         = timestamp;
        this.expiryTimestamp   = expiryTimestamp;
        this.checklist         = List.copyOf(checklist);
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {
            out.writeInt(getId());
            out.writeLong(timestamp);
            out.writeLong(expiryTimestamp);
            out.writeInt(checklist.size());
            for (String item : checklist) {
                out.writeUTF(item);
            }
            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING VEHICLE_TO_RSU_ACK", e);
        }
    }

    public long   getTimestamp()         { return timestamp; }
    public long   getExpiryTimestamp()   { return expiryTimestamp; }
    public List<String> getChecklist()   { return List.copyOf(checklist); }
    
    public String getNextHop() {
        return checklist.isEmpty() ? null : checklist.getLast();
    }

    @Override
    public String toString() {
        return "EVENT_ACK :" +
               " | ACK_ID: " + getId() +
               " | CHECKLIST_SIZE: " + checklist.size() +
               " | TIMESTAMP: " + timestamp +
               " | EXPIRY_TIMESTAMP: " + expiryTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventACK)) return false;
        EventACK eventACK = (EventACK) o;
        return getId() == eventACK.getId() &&
               timestamp == eventACK.timestamp &&
               expiryTimestamp == eventACK.expiryTimestamp &&
               checklist.equals(eventACK.checklist);
    }

    @Override
    public int hashCode() {
        int result = getId() ^ (getId() >>> 32);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (expiryTimestamp ^ (expiryTimestamp >>> 32));
        result = 31 * result + checklist.hashCode();
        return result;
    }
}
