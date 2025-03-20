package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.Messages.*;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.rti.TIME;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class VehApp extends AbstractApplication<VehicleOperatingSystem>
        implements VehicleApplication, CommunicationApplication {

    private final long MSG_DELAY = 100 * TIME.MILLI_SECOND;
    private final int TX_POWER = 50;
    private final double TX_RANGE = 500.0;
    private boolean startSending = false;
    private double vehHeading;
    private double vehSpeed;
    private int vehLane;
    private final Map<String, Double> neighborsDistance = new HashMap<>();

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER)
                .distance(TX_RANGE)
                .create());
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [INFO] VEHICLE APP STARTUP");
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        String vehId = getOs().getId().toUpperCase();
        printNeighbors();
        getLog().infoSimTime(this, "[" + vehId + "] [INFO] VEHICLE APP SHUTDOWN");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void onVehicleUpdated(@Nullable org.eclipse.mosaic.lib.objects.vehicle.VehicleData prev,
                                 @Nonnull org.eclipse.mosaic.lib.objects.vehicle.VehicleData updated) {
        vehHeading = updated.getHeading().doubleValue();
        vehSpeed = updated.getSpeed();
        vehLane = updated.getRoadPosition().getLaneIndex();
        if (!startSending) {
            startSending = true;
            String vehId = getOs().getId().toUpperCase();
            getLog().infoSimTime(this, "[" + vehId + "] [INFO] VEHICLE APP READY TO SEND VEHICLE INFO MESSAGES");
        }
    }

    @Override
    public void processEvent(Event event) {
        if (startSending) {
            sendVehInfoMsg();
            sendVehDetailMsg();
        }
        printNeighbors();
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    private void sendVehInfoMsg() {
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        long currentTime = getOs().getSimulationTime();
        V2VMsg msg = new V2VMsg(routing, currentTime, getOs().getId().toUpperCase(), getOs().getPosition());
        getOs().getAdHocModule().sendV2xMessage(msg);
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [SENT] [V2V] " + msg + " AT TIME " + currentTime);
    }

    private void sendVehDetailMsg() {
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        long currentTime = getOs().getSimulationTime();
        V2XMsg msg = new V2XMsg(routing, currentTime, "DIRECT", getOs().getId().toUpperCase(), getOs().getPosition(),
                vehHeading, vehSpeed, vehLane);
        getOs().getAdHocModule().sendV2xMessage(msg);
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [SENT] [V2X] " + msg + " AT TIME " + currentTime);
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rcvMsg) {
        String vehId = getOs().getId().toUpperCase();
        V2xMessage msg = rcvMsg.getMessage();
        if (msg instanceof V2VMsg) {
            V2VMsg vMsg = (V2VMsg) msg;
            if (!vMsg.getSenderName().equalsIgnoreCase(vehId)) {
                updateNeighborDistance(vMsg.getSenderName().toUpperCase(), computeDistance(getOs().getPosition(), vMsg.getSenderPos()));
                getLog().infoSimTime(this, "[" + vehId + "] [RECEIVED] [V2V] " + msg);
            }
        } else if (msg instanceof V2XMsg) {
            V2XMsg vxMsg = (V2XMsg) msg;
            if (!vxMsg.getVehicleId().equalsIgnoreCase(vehId)) {
                return;
            }
            getLog().infoSimTime(this, "[" + vehId + "] [RECEIVED] [V2X] " + vxMsg);
        } else if (msg instanceof F2RMsg) {
            F2RMsg f2rMsg = (F2RMsg) msg;
            if (f2rMsg.getVehicleDestinations().contains(vehId)) {
                getLog().infoSimTime(this, "[" + vehId + "] [RECEIVED] [F2R] [" + f2rMsg.getRsuDestinations() + "] ACK RECEIVED: " + f2rMsg);
            } else {
                return;
            }
        } else {
            return;
        }
    }

    private void updateNeighborDistance(String senderId, double distance) {
        neighborsDistance.put(senderId, distance);
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [INFO] UPDATED NEIGHBOR " + senderId + " WITH DISTANCE: " + String.format("%.4f", distance));
    }

    private double computeDistance(GeoPoint p1, GeoPoint p2) {
        double latDiff = p1.getLatitude() - p2.getLatitude();
        double lonDiff = p1.getLongitude() - p2.getLongitude();
        return Math.sqrt(latDiff * latDiff + lonDiff * lonDiff);
    }

    private void printNeighbors() {
        StringBuilder sb = new StringBuilder();
        String vehId = getOs().getId().toUpperCase();
        sb.append("[").append(vehId).append("] [INFO] NEIGHBORS (ID -> DISTANCE):\n");
        for (Map.Entry<String, Double> entry : neighborsDistance.entrySet()) {
            sb.append("  ").append(entry.getKey()).append(" -> ").append(String.format("%.4f", entry.getValue())).append("\n");
        }
        getLog().infoSimTime(this, sb.toString());
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {}

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {}

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {}
}