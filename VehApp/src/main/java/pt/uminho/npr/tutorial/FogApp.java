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
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.rti.TIME;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FogApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private final long MSG_DELAY = 1000 * TIME.MILLI_SECOND;
    private final int TX_POWER = 70;
    private final double TX_RANGE = 1500.0;
    private final Map<String, R2FMsg> receivedBigArchive = new HashMap<>();
    private final Map<String, F2RMsg> sentBigArchive = new HashMap<>();

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
        String fogId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + fogId + "] [INFO] FOGAPP STARTUP - ACTIVE AT POSITION: " + getOs().getPosition());
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        String fogId = getOs().getId().toUpperCase();
        printMessageHistory();
        getLog().infoSimTime(this, "[" + fogId + "] [INFO] FOGAPP SHUTDOWN");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) throws Exception {
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rx) {
        String fogId = getOs().getId().toUpperCase();
        V2xMessage msg = rx.getMessage();
        if (msg instanceof R2FMsg) {
            R2FMsg r2fMsg = (R2FMsg) msg;
            String rsuSender = r2fMsg.getRsuName().toUpperCase();
            String recKey = rsuSender + "-" + r2fMsg.getReceiver().toUpperCase() + "-" + r2fMsg.getTimeStamp();
            receivedBigArchive.put(recKey, r2fMsg);
            getLog().infoSimTime(this, "[" + fogId + "] [RECEIVED] [R2F] FROM [" + rsuSender + "] STORED R2FMSG WITH KEY: " + recKey);
            long time = getOs().getSimulationTime();
            F2RMsg f2rMsg = new F2RMsg(
                    getOs().getAdHocModule().createMessageRouting().viaChannel(AdHocChannel.CCH).topoBroadCast(),
                    time,
                    "ACK",
                    fogId,
                    Collections.singletonList(rsuSender),
                    Collections.singletonList("VEH_0")
            );
            getOs().getAdHocModule().sendV2xMessage(f2rMsg);
            String sentKey = fogId + "-" + rsuSender + "-" + f2rMsg.getTimeStamp();
            sentBigArchive.put(sentKey, f2rMsg);
            getLog().infoSimTime(this, "[" + fogId + "] [SENT] [F2R] SENT ACK TO RSU [" + rsuSender + "] WITH KEY: " + sentKey);
        } else {
            return;
        }
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        String fogId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + fogId + "] [INFO] MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        String fogId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + fogId + "] [INFO] ACKNOWLEDGEMENT RECEIVED: " + ack);
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        String fogId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + fogId + "] [INFO] CAM EVENT TRIGGERED");
    }

    private void printMessageHistory() {
        StringBuilder sb = new StringBuilder();
        String fogId = getOs().getId().toUpperCase();
        sb.append("[").append(fogId).append("] [INFO] RECEIVED BIG ARCHIVE:\n");
        for (Map.Entry<String, R2FMsg> entry : receivedBigArchive.entrySet()) {
            sb.append("KEY: ").append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
        }
        sb.append("[").append(fogId).append("] [INFO] SENT BIG ARCHIVE:\n");
        for (Map.Entry<String, F2RMsg> entry : sentBigArchive.entrySet()) {
            sb.append("KEY: ").append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
        }
        getLog().infoSimTime(this, sb.toString());
    }
}