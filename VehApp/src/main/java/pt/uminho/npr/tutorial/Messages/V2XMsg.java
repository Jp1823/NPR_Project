package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.SerializationUtils;

public class V2XMsg extends V2xMessage {
    private final EncodedPayload payload;
    private final long timeStamp;
    private final String rsuName;
    private final String vehicleId;
    private final GeoPoint vehiclePos;
    private final double vehicleHeading;
    private final double vehicleSpeed;
    private final int vehicleLane;

    public V2XMsg(MessageRouting routing, long time, String rsuForwarder, String vehId,
                  GeoPoint vehPos, double heading, double speed, int lane) {
        super(routing);
        this.timeStamp = time;
        this.rsuName = rsuForwarder;
        this.vehicleId = vehId;
        this.vehiclePos = vehPos;
        this.vehicleHeading = heading;
        this.vehicleSpeed = speed;
        this.vehicleLane = lane;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(rsuName);
            dos.writeUTF(vehicleId);
            SerializationUtils.encodeGeoPoint(dos, vehiclePos);
            dos.writeDouble(vehicleHeading);
            dos.writeDouble(vehicleSpeed);
            dos.writeInt(vehicleLane);
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch(IOException e) {
            throw new RuntimeException("ERROR SERIALIZING V2XMSG", e);
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

    public String getRsuName() {
        return rsuName;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public GeoPoint getVehiclePos() {
        return vehiclePos;
    }

    public double getVehicleHeading() {
        return vehicleHeading;
    }

    public double getVehicleSpeed() {
        return vehicleSpeed;
    }

    public int getVehicleLane() {
        return vehicleLane;
    }

    @Override
    public String toString() {
        return "V2X MESSAGE - TIME: " + timeStamp +
               " | RSU: " + rsuName +
               " | VEHICLE: " + vehicleId +
               " | POSITION: " + vehiclePos +
               " | HEADING: " + vehicleHeading +
               " | SPEED: " + vehicleSpeed +
               " | LANE: " + vehicleLane;
    }
}