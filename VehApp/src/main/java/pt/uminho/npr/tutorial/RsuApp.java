package pt.uminho.npr.tutorial;
import pt.uminho.npr.tutorial.Messages.*;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.rti.TIME;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import java.util.Collections;
import java.util.List;

public class RsuApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private final long MSG_DELAY = 500 * TIME.MILLI_SECOND;
    private final int TX_POWER = 70;
    private final double TX_RANGE = 2000.0;

    private void sendFogMsg(V2XMsg vxMsg) {
        long time = getOs().getSimulationTime();
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        R2FMsg msg = new R2FMsg(routing, time, getOs().getId().toUpperCase(), "FogNode", vxMsg);
        getOs().getAdHocModule().sendV2xMessage(msg);
        getLog().infoSimTime(this, "[" + getOs().getId().toUpperCase() + "] [SENT] [FOG] " + msg);
    }

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(
          new AdHocModuleConfiguration()
              .addRadio()
              .channel(AdHocChannel.CCH)
              .power(TX_POWER)
              .distance(TX_RANGE)
              .create()
        );
        String rsuId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO] RSU APP STARTUP - ACTIVE AT POSITION: " + getOs().getPosition());
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        String rsuId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO] RSU APP SHUTDOWN");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) throws Exception {
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rcvMsg) {
        String rsuId = getOs().getId().toUpperCase();
        V2xMessage msg = rcvMsg.getMessage();
        if (msg instanceof V2XMsg) {
            V2XMsg vxMsg = (V2XMsg) msg;
            sendFogMsg(vxMsg);
            getLog().infoSimTime(this, "[" + rsuId + "] [RECEIVED] [V2X] " + vxMsg);
        } else if (msg instanceof F2RMsg) {
            F2RMsg f2rMsg = (F2RMsg) msg;
            if (f2rMsg.getRsuDestinations().contains(rsuId)) {
                if ("ACK".equalsIgnoreCase(f2rMsg.getMessageType())) {
                    List<String> vehicles = f2rMsg.getVehicleDestinations();
                    for (String vehicle : vehicles) {
                        long time = getOs().getSimulationTime();
                        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                                .viaChannel(AdHocChannel.CCH)
                                .topoBroadCast();
                        F2RMsg forwardMsg = new F2RMsg(
                                routing,
                                time,
                                "ACK",
                                getOs().getId().toUpperCase(),
                                Collections.singletonList(vehicle),
                                Collections.emptyList()
                        );
                        getOs().getAdHocModule().sendV2xMessage(forwardMsg);
                        getLog().infoSimTime(this, "[" + rsuId + "] [SENT] [F2R] SENT ACK TO VEHICLE [" + vehicle + "]: " + forwardMsg);
                    }
                    getLog().infoSimTime(this, "[" + rsuId + "] [RECEIVED] [F2R] ACK RECEIVED FOR RSU [" + rsuId + "]: " + f2rMsg);
                }
            } else {
                return;
            }
        } else {
            return;
        }
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        String rsuId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO] MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        String rsuId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO] ACKNOWLEDGEMENT RECEIVED");
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        String rsuId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO] CAM EVENT TRIGGERED");
    }
}