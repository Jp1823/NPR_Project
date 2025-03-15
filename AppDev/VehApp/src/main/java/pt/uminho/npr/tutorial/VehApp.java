package pt.uminho.npr.tutorial;

import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleData;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class VehApp extends AbstractApplication<VehicleOperatingSystem>
                   implements VehicleApplication, CommunicationApplication {

    private final long MSG_DELAY = 100 * TIME.MILLI_SECOND; 
    private final int TX_POWER = 50;
    private final double TX_RANGE = 500.0;
    private final long NEIGHBOR_TIMEOUT = 100000;
    private boolean startSending = false;
    private double vehHeading;
    private double vehSpeed;
    private int vehLane;

    private static class NeighborRecord {
        long lastHeardTime;
        double heading;
        double speed;
        int lane;
    }

    private final Map<String, NeighborRecord> neighborsLdm = new HashMap<>();

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER)
                .distance(TX_RANGE)
                .create());
        getLog().infoSimTime(this, "VEHAPP ON STARTUP");
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        getLog().infoSimTime(this, "VEHAPP ON SHUTDOWN");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void onVehicleUpdated(@Nullable VehicleData prev, @Nonnull VehicleData updated) {
        vehHeading = updated.getHeading().doubleValue();
        vehSpeed = updated.getSpeed();
        vehLane = updated.getRoadPosition().getLaneIndex();
        if (!startSending) {
            startSending = true;
            getLog().infoSimTime(this, "VEHAPP READY TO SEND VEHINFO");
        }
    }

    @Override
    public void processEvent(Event event) {
        if (startSending) {
            sendVehInfoMsg();
        }
        cleanInactiveNeighbors();
        printNeighborList();
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    private void sendVehInfoMsg() {
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        long time = getOs().getSimulationTime();
        VehInfoMsg msg = new VehInfoMsg(
            routing,
            time,
            getOs().getId(),
            getOs().getPosition(),
            vehHeading,
            vehSpeed,
            vehLane,
            "PLATE-ABC123",
            "BRAND-XYZ",
            "DRIVER-JOHN"
        );
        getOs().getAdHocModule().sendV2xMessage(msg);
        getLog().infoSimTime(this, "VEHAPP SENT VEHINFOMSG: " + msg + " AT TIME " + time);
    }

    private void cleanInactiveNeighbors() {
        long now = getOs().getSimulationTime();
        Iterator<Map.Entry<String, NeighborRecord>> it = neighborsLdm.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, NeighborRecord> e = it.next();
            if ((now - e.getValue().lastHeardTime) > NEIGHBOR_TIMEOUT) {
                getLog().infoSimTime(this, "VEHAPP REMOVING INACTIVE NEIGHBOR: " + e.getKey());
                it.remove();
            }
        }
    }

    private void printNeighborList() {
        StringBuilder sb = new StringBuilder("VEHAPP NEIGHBORS: [");
        for (Map.Entry<String, NeighborRecord> e : neighborsLdm.entrySet()) {
            NeighborRecord r = e.getValue();
            sb.append(e.getKey())
              .append("(h=").append(r.heading)
              .append(",s=").append(r.speed)
              .append(",l=").append(r.lane)
              .append(") ");
        }
        sb.append("]");
        getLog().infoSimTime(this, sb.toString());
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rcvMsg) {
        if (rcvMsg.getMessage() instanceof VehInfoMsg) {
            VehInfoMsg vMsg = (VehInfoMsg) rcvMsg.getMessage();
            if (!vMsg.getSenderName().equals(getOs().getId())) {
                handleVehInfoMsg(vMsg);
            }
        }
        getLog().infoSimTime(this, "VEHAPP RECEIVED: " + rcvMsg.getMessage());
    }

    private void handleVehInfoMsg(VehInfoMsg vehMsg) {
        String otherVehId = vehMsg.getSenderName();
        NeighborRecord rec = neighborsLdm.get(otherVehId);
        if (rec == null) {
            rec = new NeighborRecord();
            neighborsLdm.put(otherVehId, rec);
            getLog().infoSimTime(this, "VEHAPP NEW NEIGHBOR: " + otherVehId);
        }
        rec.lastHeardTime = vehMsg.getTimeStamp();
        rec.heading = vehMsg.getSenderHeading();
        rec.speed = vehMsg.getSenderSpeed();
        rec.lane = vehMsg.getSenderLaneId();
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        getLog().infoSimTime(this, "VEHAPP ON MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        getLog().infoSimTime(this, "VEHAPP ON ACK RECEIVED");
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        getLog().infoSimTime(this, "VEHAPP ON CAM BUILDING");
    }
}