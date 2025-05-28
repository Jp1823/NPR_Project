package pt.uminho.npr.projeto.messages;

import java.io.*;
import java.util.*;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.*;
import org.eclipse.mosaic.lib.util.SerializationUtils;

import pt.uminho.npr.projeto.records.NodeRecord;

public final class CamMessage extends V2xMessage {

    private final long timeStamp;
    private final String senderId;
    private final GeoPoint position;
    private final double heading;
    private final double speed;
    private final double acceleration;
    private final boolean brakeLightOn;
    private final boolean leftTurnSignalOn;
    private final boolean rightTurnSignalOn;
    private final int timeToLive;
    private final Map<String, NodeRecord> neighborGraph;

    public CamMessage(MessageRouting routing,
                        long timeStamp,
                        String senderId,
                        GeoPoint position,
                        double heading,
                        double speed,
                        double acceleration,
                        boolean brakeLightOn,
                        boolean leftTurnSignalOn,
                        boolean rightTurnSignalOn,
                        int timeToLive,
                        Map<String, NodeRecord> neighborGraph) {
        super(routing);
        this.timeStamp           = timeStamp;
        this.senderId            = Objects.requireNonNull(senderId);
        this.position            = Objects.requireNonNull(position);
        this.heading             = heading;
        this.speed               = speed;
        this.acceleration        = acceleration;
        this.brakeLightOn        = brakeLightOn;
        this.leftTurnSignalOn    = leftTurnSignalOn;
        this.rightTurnSignalOn   = rightTurnSignalOn;
        this.timeToLive          = timeToLive;
        this.neighborGraph       = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(neighborGraph)));
    }

    @Nonnull @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf)) {
            out.writeLong(timeStamp);
            out.writeUTF(senderId);
            SerializationUtils.encodeGeoPoint(out, position);
            out.writeDouble(heading);
            out.writeDouble(speed);
            out.writeDouble(acceleration);
            out.writeBoolean(brakeLightOn);
            out.writeBoolean(leftTurnSignalOn);
            out.writeBoolean(rightTurnSignalOn);
            out.writeInt(timeToLive);
            out.writeInt(neighborGraph.size());
            for (Map.Entry<String, NodeRecord> entry : neighborGraph.entrySet()) {
                String nodeId = entry.getKey();
                NodeRecord record = entry.getValue();
                out.writeUTF(nodeId);
                out.writeDouble(record.getDistanceFromVehicle());
                out.writeDouble(record.getDistanceToClosestRsu());
                out.writeBoolean(record.isReachableToRsu());
                List<String> reachable = record.getReachableNeighbors();
                out.writeInt(reachable.size());
                for (String id : reachable) {
                    out.writeUTF(id);
                }
                List<String> direct = record.getDirectNeighbors();
                out.writeInt(direct.size());
                for (String id : direct) {
                    out.writeUTF(id);
                }
                out.writeLong(record.getCreationTimestamp());
            }
            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING VEHICLE_TO_VEHICLE", e);
        }
    }

    public long   getTimeStamp()            { return timeStamp; }
    public String getSenderId()             { return senderId; }
    public GeoPoint getPosition()           { return position; }
    public double getHeading()              { return heading; }
    public double getSpeed()                { return speed; }
    public double getAcceleration()         { return acceleration; }
    public boolean isBrakeLightOn()         { return brakeLightOn; }
    public boolean isLeftTurnSignalOn()     { return leftTurnSignalOn; }
    public boolean isRightTurnSignalOn()    { return rightTurnSignalOn; }
    public int    getTimeToLive()           { return timeToLive; }
    public Map<String, NodeRecord> getNeighborGraph() { return neighborGraph; }

    @Override
    public String toString() {
        return "VEHICLE_TO_VEHICLE :" +
               " | SENDER_ID: " + senderId +
               " | POSITION: " + position +
               " | HEADING: " + heading +
               " | SPEED: " + speed +
               " | ACCELERATION: " + acceleration +
               " | BRAKE_LIGHT_ON: " + brakeLightOn +
               " | LEFT_TURN_SIGNAL_ON: " + leftTurnSignalOn +
               " | RIGHT_TURN_SIGNAL_ON: " + rightTurnSignalOn +
               " | TIME_TO_LIVE: " + timeToLive +
               " | NEIGHBOR_GRAPH_SIZE: " + neighborGraph.size() +
               " | TIMESTAMP: " + timeStamp;
    }
}
