package pt.uminho.npr.projeto.messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;

import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

public abstract class EventMessage extends V2xMessage {

    private final long   timestamp;
    private final long   expiryTimestamp;
    private final String target;
    private final List<String> forwardingTrail;

    protected EventMessage(MessageRouting routing,
                           int eventId,
                           long timestamp,
                           long expiryTimestamp,
                           String target,
                           List<String> forwardingTrail) {

        super(routing, eventId);
        this.timestamp       = timestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.target          = target;
        this.forwardingTrail = forwardingTrail;
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {
            out.writeInt(getId());
            out.writeLong(timestamp);
            out.writeLong(expiryTimestamp);
            out.writeUTF(target);
            for (String nodeId : forwardingTrail) {
                out.writeUTF(nodeId);
            }

            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING FOG_EVENT_MESSAGE", e);
        }
    }

    public long         getTimestamp()       { return timestamp; }
    public long         getExpiryTimestamp() { return expiryTimestamp; }
    public String       getTarget()          { return target; }
    public List<String> getForwardingTrail() { return forwardingTrail; }

    public String getNextHop() {
        return forwardingTrail.isEmpty() ? null : forwardingTrail.getLast();
    }

    public boolean hasNextHop() {
        return !forwardingTrail.isEmpty();
    }

    @Override
    public String toString() {
        return "EVENT_MESSAGE :" +
               " | EVENT_ID: "       + getId() +
               " | TARGET: "           + target +
               " | TIMESTAMP: "        + timestamp +
               " | EXPIRY_TIMESTAMP: " + expiryTimestamp +
               " | FORWARDING_TRAIL: " + forwardingTrail;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventMessage)) return false;
        EventMessage that = (EventMessage) o;
        return getId() == that.getId() &&
               timestamp == that.timestamp &&
               expiryTimestamp == that.expiryTimestamp &&
               target.equals(that.target) &&
               forwardingTrail.equals(that.forwardingTrail);
    }

    @Override
    public int hashCode() {
        int result = getId() ^ (getId() >>> 32);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (expiryTimestamp ^ (expiryTimestamp >>> 32));
        result = 31 * result + target.hashCode();
        result = 31 * result + forwardingTrail.hashCode();
        return result;
    }
}