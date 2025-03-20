package pt.uminho.npr.tutorial.Messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

public class F2RMsg extends V2xMessage {
    private final long timeStamp;
    private final String messageType;
    private final String fogNodeId;
    private final List<String> rsuDestinations;
    private final List<String> vehicleDestinations;

    public F2RMsg(MessageRouting routing, long time, String messageType, String fogNodeId,
                  List<String> rsuDestinations, List<String> vehicleDestinations) {
        super(routing);
        this.timeStamp = time;
        this.messageType = messageType;
        this.fogNodeId = fogNodeId;
        this.rsuDestinations = rsuDestinations;
        this.vehicleDestinations = vehicleDestinations;
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(messageType);
            dos.writeUTF(fogNodeId);
            dos.writeInt(rsuDestinations.size());
            for(String rsu : rsuDestinations) {
                dos.writeUTF(rsu);
            }
            dos.writeInt(vehicleDestinations.size());
            for(String veh : vehicleDestinations) {
                dos.writeUTF(veh);
            }
            return new EncodedPayload(baos.toByteArray(), baos.size());
        } catch(IOException e) {
            throw new RuntimeException("ERROR ENCODING F2RMSG", e);
        }
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getFogNodeId() {
        return fogNodeId;
    }

    public List<String> getRsuDestinations() {
        return rsuDestinations;
    }

    public List<String> getVehicleDestinations() {
        return vehicleDestinations;
    }

    @Override
    public String toString() {
        return "F2R MESSAGE - TIME: " + timeStamp +
               " | TYPE: " + messageType +
               " | FOG_NODE: " + fogNodeId +
               " | RSU_DESTINATIONS: " + rsuDestinations +
               " | VEHICLE_DESTINATIONS: " + vehicleDestinations;
    }
}