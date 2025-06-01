package pt.uminho.npr.projeto.messages;

import java.io.*;
import java.util.*;

import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.*;
import org.eclipse.mosaic.lib.util.SerializationUtils;

public final class CamMessage extends V2xMessage {

    private final String vehId;
    private final long timestamp;
    private final int hopsToLive;
    private final GeoPoint position;
    private final Set<String> neighbors;
    //private final double heading;
    //private final double speed;

    public CamMessage(MessageRouting routing,
                        int camId,
                        String vehId,
                        long timestamp,
                        int hopsToLive,
                        GeoPoint position,
                        Set<String> neighbors) {

        super(routing, camId);
        this.vehId = vehId;
        this.timestamp = timestamp;
        this.hopsToLive = hopsToLive;
        this.position = position;
        this.neighbors = neighbors;
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf)) {
            out.writeInt(this.getId());
            out.writeUTF(vehId);
            out.writeLong(timestamp);
            out.writeInt(hopsToLive);
            SerializationUtils.encodeGeoPoint(out, position);
            out.writeInt(neighbors.size());
            for (String neighborId : neighbors) {
                out.writeUTF(neighborId);
            }
            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING VEHICLE_TO_VEHICLE", e);
        }
    }

    public String   getVehId()      { return vehId; }
    public long     getTimestamp()  { return timestamp; }
    public int      getHopsToLive() { return hopsToLive; }
    public GeoPoint getPosition()   { return position; }
    public Set<String> getNeighbors() { return neighbors; }

    @Override
    public String toString() {
        return "VEHICLE_TO_VEHICLE :" +
               " | ID: " + this.getId() +
               " | VEHICLE_ID: " + vehId +
               " | TIMESTAMP: " + timestamp +
               " | HOPS_TO_LIVE: " + hopsToLive +
               " | POSITION: " + position +
               " | NEIGHBORS_SIZE: " + neighbors.size();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CamMessage)) return false;
        CamMessage other = (CamMessage) obj;
        return this.getId() == other.getId() &&
               this.vehId.equals(other.vehId) &&
               this.timestamp == other.timestamp &&
               this.hopsToLive == other.hopsToLive &&
               this.position.equals(other.position) &&
               this.neighbors.equals(other.neighbors);
    }

    @Override
    public int hashCode() {
        int result = getId() ^ (getId() >>> 32);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (hopsToLive ^ (hopsToLive >>> 32));
        result = 31 * result + position.hashCode();
        result = 31 * result + neighbors.hashCode();
        return result;
    }
}
