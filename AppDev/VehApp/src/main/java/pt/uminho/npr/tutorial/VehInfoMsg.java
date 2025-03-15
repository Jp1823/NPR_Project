package pt.uminho.npr.tutorial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.SerializationUtils;

public class VehInfoMsg extends V2xMessage {
    private final EncodedPayload payload;
    private final long timeStamp;
    private final String senderName;
    private final GeoPoint senderPos;
    private final double senderHeading;
    private final double senderSpeed;
    private final int senderLaneId;
    private final String plateNumber;
    private final String vehicleBrand;
    private final String driverName;

    public VehInfoMsg(MessageRouting routing, long time, String name, GeoPoint pos,
                      double heading, double speed, int laneId,
                      String plate, String brand, String driver) {
        super(routing);
        this.timeStamp = time;
        this.senderName = name;
        this.senderPos = pos;
        this.senderHeading = heading;
        this.senderSpeed = speed;
        this.senderLaneId = laneId;
        this.plateNumber = plate;
        this.vehicleBrand = brand;
        this.driverName = driver;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(senderName);
            SerializationUtils.encodeGeoPoint(dos, senderPos);
            dos.writeDouble(senderHeading);
            dos.writeDouble(senderSpeed);
            dos.writeInt(senderLaneId);
            dos.writeUTF(plateNumber);
            dos.writeUTF(vehicleBrand);
            dos.writeUTF(driverName);
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR SERIALIZING VEHINFOMSG", e);
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

    public double getSenderHeading() {
        return senderHeading;
    }

    public double getSenderSpeed() {
        return senderSpeed;
    }

    public int getSenderLaneId() {
        return senderLaneId;
    }

    public String getPlateNumber() {
        return plateNumber;
    }

    public String getVehicleBrand() {
        return vehicleBrand;
    }

    public String getDriverName() {
        return driverName;
    }

    @Override
    public String toString() {
        return "VEHINFOMSG{TIMESTAMP=" + timeStamp + ", SENDERNAME='" + senderName + "', POS=" + senderPos +
               ", HEADING=" + senderHeading + ", SPEED=" + senderSpeed + ", LANEID=" + senderLaneId +
               ", PLATE='" + plateNumber + "', BRAND='" + vehicleBrand + "', DRIVER='" + driverName + "'}";
    }
}
