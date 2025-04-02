package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.SerializationUtils;

public class V2VExt extends V2xMessage {

    private final EncodedPayload payload;
    private final long timeStamp;
    private final String senderName;
    private final GeoPoint senderPos;
    private final double heading;
    private final double speed;
    private final int lane;
    private final List<String> neighborList;

    public V2VExt(MessageRouting routing,
                  long timeStamp,
                  String senderName,
                  GeoPoint senderPos,
                  double heading,
                  double speed,
                  int lane,
                  List<String> neighborList) {
        super(routing);
        this.timeStamp = timeStamp;
        this.senderName = senderName;
        this.senderPos = senderPos;
        this.heading = heading;
        this.speed = speed;
        this.lane = lane;
        this.neighborList = neighborList;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            // Logical order: timeStamp, senderName, senderPos, heading, speed, lane, neighborList
            dos.writeLong(timeStamp);
            dos.writeUTF(senderName);
            SerializationUtils.encodeGeoPoint(dos, senderPos);
            dos.writeDouble(heading);
            dos.writeDouble(speed);
            dos.writeInt(lane);
            dos.writeInt(neighborList.size());
            for (String neighbor : neighborList) {
                dos.writeUTF(neighbor);
            }
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR SERIALIZING V2VEXT", e);
        }
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        return payload;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getSenderName() {
        return senderName;
    }

    public GeoPoint getSenderPos() {
        return senderPos;
    }

    public double getHeading() {
        return heading;
    }

    public double getSpeed() {
        return speed;
    }

    public int getLane() {
        return lane;
    }

    public List<String> getNeighborList() {
        return neighborList;
    }

    @Override
    public String toString() {
        return "V2VEXT MESSAGE - " +
               "TIME: " + timeStamp +
               " | SENDER: " + senderName +
               " | POSITION: " + senderPos +
               " | HEADING: " + heading +
               " | SPEED: " + speed +
               " | LANE: " + lane +
               " | NEIGHBORS: " + neighborList;
    }
}
