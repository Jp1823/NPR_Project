package pt.uminho.npr.tutorial;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MyRsuApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private final long MSG_DELAY = 500 * TIME.MILLI_SECOND;
    private final int TX_POWER = 70;
    private final double TX_RANGE = 2000.0;
    private static final long VEHICLE_TIMEOUT = 30000; // 30 seconds

    private static class VehicleRecord {
        long lastHeardTime;
        double heading;
        double speed;
        int lane;
    }

    private final Map<String, VehicleRecord> connectedVehicles = new HashMap<>();

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER)
                .distance(TX_RANGE)
                .create());
        getLog().infoSimTime(this, "MYRSUAPP ON STARTUP: RSU ACTIVE AT POSITION " + getOs().getPosition());
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        getLog().infoSimTime(this, "MYRSUAPP ON SHUTDOWN: RSU TERMINATED");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) throws Exception {
        sendRsuInfoMsg();
        cleanInactiveVehicles();
        StringBuilder sb = new StringBuilder("VEHICLES CONNECTED: [");
        for (String vehId : connectedVehicles.keySet()) {
            sb.append(vehId).append(" ");
        }
        sb.append("]");
        getLog().infoSimTime(this, "MYRSUAPP PROCESS EVENT: " + sb.toString());
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    private void sendRsuInfoMsg() {
        long time = getOs().getSimulationTime();
        MyRsuInfoMsg msg = new MyRsuInfoMsg(
                getOs().getAdHocModule().createMessageRouting()
                        .viaChannel(AdHocChannel.CCH)
                        .topoBroadCast(),
                time,
                getOs().getId(),
                getOs().getPosition()
        );
        getOs().getAdHocModule().sendV2xMessage(msg);
        getLog().infoSimTime(this, "MYRSUAPP SENT MYRSUINFOMSG: " + msg.toString() + " AT SIM TIME " + time);
    }

    private void cleanInactiveVehicles() {
        long now = getOs().getSimulationTime();
        Iterator<Map.Entry<String, VehicleRecord>> it = connectedVehicles.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, VehicleRecord> entry = it.next();
            if ((now - entry.getValue().lastHeardTime) > VEHICLE_TIMEOUT) {
                getLog().infoSimTime(this, "MYRSUAPP REMOVING INACTIVE VEHICLE: " + entry.getKey());
                it.remove();
            }
        }
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage receivedMsg) {
        V2xMessage msg = receivedMsg.getMessage();
        if (msg instanceof VehInfoMsg) {
            VehInfoMsg vehMsg = (VehInfoMsg) msg;
            handleVehInfoMsg(vehMsg);
        } else if (msg instanceof MyRsuInfoMsg) {
            MyRsuInfoMsg rsuMsg = (MyRsuInfoMsg) msg;
            getLog().infoSimTime(this, "MYRSUAPP RECEIVED MYRSUINFOMSG: " + rsuMsg.toString());
        } else {
            getLog().infoSimTime(this, "MYRSUAPP RECEIVED UNKNOWN MESSAGE: " + msg.toString());
        }
    }

    private void handleVehInfoMsg(VehInfoMsg vehMsg) {
        String vehId = vehMsg.getSenderName();
        getLog().infoSimTime(this, "MYRSUAPP RECEIVED VEHINFOMSG FROM: " + vehId);
        VehicleRecord record = connectedVehicles.get(vehId);
        if (record == null) {
            record = new VehicleRecord();
            connectedVehicles.put(vehId, record);
            getLog().infoSimTime(this, "MYRSUAPP ADDED NEW VEHICLE: " + vehId);
            StringBuilder sb = new StringBuilder("MYRSUAPP TOTAL VEHICLE LIST: [");
            for (String id : connectedVehicles.keySet()) {
                sb.append(id).append(" ");
            }
            sb.append("]");
            getLog().infoSimTime(this, sb.toString());
        }
        record.lastHeardTime = vehMsg.getTimeStamp();
        record.heading = vehMsg.getSenderHeading();
        record.speed = vehMsg.getSenderSpeed();
        record.lane = vehMsg.getSenderLaneId();
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission transmission) {
        getLog().infoSimTime(this, "MYRSUAPP ON MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        getLog().infoSimTime(this, "MYRSUAPP ON ACKNOWLEDGEMENT RECEIVED: " + ack.toString());
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        getLog().infoSimTime(this, "MYRSUAPP ON CAM BUILDING");
    }
}