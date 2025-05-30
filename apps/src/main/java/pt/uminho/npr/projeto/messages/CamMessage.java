package pt.uminho.npr.projeto.messages;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.*;
import org.eclipse.mosaic.lib.util.SerializationUtils;

import pt.uminho.npr.projeto.records.NodeRecord;

public final class CamMessage extends V2xMessage {

    private final String vehId;
    private final long timestamp;
    private final int timeToLive;
    private final GeoPoint position;
    //private final double   heading;
    //private final double   speed;
    private final Map<String, NodeRecord> neighborsGraph;

    public CamMessage(MessageRouting routing,
                        int camId,
                        String vehId,
                        long timestamp,
                        int timeToLive,
                        GeoPoint position,
                        Map<String, NodeRecord> neighborsGraph) {

        super(routing, camId);
        this.vehId = vehId;
        this.timestamp = timestamp;
        this.timeToLive = timeToLive;
        this.position = position;
        this.neighborsGraph = neighborsGraph;
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf)) {
            out.writeInt(this.getId());
            out.writeUTF(vehId);
            out.writeLong(timestamp);
            out.writeInt(timeToLive);
            SerializationUtils.encodeGeoPoint(out, position);
            out.writeInt(neighborsGraph.size());
            for (Entry<String, NodeRecord> entry : neighborsGraph.entrySet()) {
                String nodeId = entry.getKey();
                NodeRecord nodeRecord = entry.getValue();
                out.writeUTF(nodeId);
                out.writeDouble(nodeRecord.getDistanceFromVehicle());
                out.writeDouble(nodeRecord.getDistanceToClosestRsu());
                out.writeBoolean(nodeRecord.isReachableToRsu());
                List<String> reachable = nodeRecord.getReachableNeighbors();
                out.writeInt(reachable.size());
                for (String id : reachable) {
                    out.writeUTF(id);
                }
                List<String> direct = nodeRecord.getDirectNeighbors();
                out.writeInt(direct.size());
                for (String id : direct) {
                    out.writeUTF(id);
                }
                out.writeLong(nodeRecord.getCreationTimestamp());
            }
            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING VEHICLE_TO_VEHICLE", e);
        }
    }

    public String   getVehId()      { return vehId; }
    public long     getTimestamp()  { return timestamp; }
    public int      getTimeToLive() { return timeToLive; }
    public GeoPoint getPosition()   { return position; }
    public Map<String, NodeRecord> getNeighborsGraph() { return neighborsGraph; }

    @Override
    public String toString() {
        return "VEHICLE_TO_VEHICLE :" +
               " | ID: " + this.getId() +
               " | VEHICLE_ID: " + vehId +
               " | TIMESTAMP: " + timestamp +
               " | TIME_TO_LIVE: " + timeToLive +
               " | POSITION: " + position +
               " | NEIGHBOR_GRAPH_SIZE: " + neighborsGraph.size();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CamMessage)) return false;
        CamMessage other = (CamMessage) obj;
        return this.getId() == other.getId() &&
               this.vehId.equals(other.vehId) &&
               this.timestamp == other.timestamp &&
               this.timeToLive == other.timeToLive &&
               this.position.equals(other.position) &&
               this.neighborsGraph.equals(other.neighborsGraph);
    }

    @Override
    public int hashCode() {
        int result = getId() ^ (getId() >>> 32);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (timeToLive ^ (timeToLive >>> 32));
        result = 31 * result + position.hashCode();
        result = 31 * result + neighborsGraph.hashCode();
        return result;
    }
}
