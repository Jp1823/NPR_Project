package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

public class R2VMsg extends V2xMessage {
    private final String uniqueId;
    private final long timeStamp;
    private final long timestampLimit;
    private final String vehDestination;
    private final String nextHop;
    private final String order;
    private final List<String> forwardingTrail;

    public R2VMsg(MessageRouting routing, String uniqueId, long timeStamp, long timestampLimit,
                  String vehDestination, String nextHop, String order, List<String> forwardingTrail) {
        super(routing);
        this.uniqueId = uniqueId;
        this.timeStamp = timeStamp;
        this.timestampLimit = timestampLimit;
        this.vehDestination = vehDestination;
        this.nextHop = nextHop;
        this.order = order;
        this.forwardingTrail = forwardingTrail;
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(uniqueId);
            dos.writeLong(timeStamp);
            dos.writeLong(timestampLimit);
            dos.writeUTF(vehDestination);
            dos.writeUTF(nextHop);
            dos.writeUTF(order);
            dos.writeInt(forwardingTrail.size());
            for (String node : forwardingTrail) {
                dos.writeUTF(node);
            }
            return new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING R2VMSG", e);
        }
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
    
    public long getTimestampLimit() {
        return timestampLimit;
    }

    public String getVehDestination() {
        return vehDestination;
    }

    public String getNextHop() {
        return nextHop;
    }

    public String getOrder() {
        return order;
    }
    
    public List<String> getForwardingTrail() {
        return forwardingTrail;
    }

    @Override
    public String toString() {
        return "R2V MESSAGE - UNIQUEID: " + uniqueId +
               " | TIME: " + timeStamp +
               " | TIMESTAMP_LIMIT: " + timestampLimit +
               " | VEH_DESTINATION: " + vehDestination +
               " | NEXT_HOP: " + nextHop +
               " | ORDER: " + order +
               " | FORWARDING_TRAIL: " + forwardingTrail;
    }
}
