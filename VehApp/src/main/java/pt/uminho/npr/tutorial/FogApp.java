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
import org.eclipse.mosaic.rti.TIME;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import java.util.HashMap;
import java.util.Map;

/**
 * FOGAPP IMPLEMENTATION FOR V2X COMMUNICATION.
 * THIS APPLICATION OPERATES AS A FOG NODE, SENDING COMMANDS (F2R MESSAGES) TO RSUS AND RECEIVING RESPONSES (R2F MESSAGES).
 * LOGGING IS STANDARDIZED WITH INFO FOR CRITICAL EVENTS AND DEBUG FOR DETAILED INFORMATION.
 */
public class FogApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    // CONSTANTS
    private final long MSG_DELAY = 1000 * TIME.MILLI_SECOND;
    private final int TX_POWER = 70;
    private final double TX_RANGE = 100.0;

    // MESSAGE ARCHIVES
    private final Map<String, R2FMsg> receivedBigArchive = new HashMap<>();
    private final Map<String, F2RMsg> sentBigArchive = new HashMap<>();

    /**
     * LOGS A MESSAGE AT THE INFO LEVEL (CRITICAL EVENTS, INTERNAL STRUCTURES, ETC.).
     *
     * @param message THE MESSAGE TO LOG.
     */
    private void logInfo(String message) {
        String fogId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + fogId + "] [INFO] " + message.toUpperCase());
    }

    /**
     * LOGS A MESSAGE AT THE DEBUG LEVEL (DETAILED PROCESSING INFORMATION).
     *
     * @param message THE MESSAGE TO LOG.
    private void logDebug(String message) {
        String fogId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + fogId + "] [DEBUG] " + message.toUpperCase());
    }
    */

    @Override
    public void onStartup() {
        // ENABLE AD-HOC RADIO MODULE
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER)
                .distance(TX_RANGE)
                .create()
        );
        // LOG FOGAPP STARTUP WITH POSITION INFORMATION
        logInfo("FOGAPP STARTUP - ACTIVE AT POSITION: " + getOs().getPosition());
        // SCHEDULE FIRST EVENT
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        // PRINT MESSAGE HISTORY BEFORE SHUTDOWN
        printMessageHistory();
        logInfo("FOGAPP SHUTDOWN");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) throws Exception {
        sendRandomF2RMsg();
        // SCHEDULE NEXT EVENT
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    /**
     * CREATES AND SENDS A RANDOM F2R MESSAGE (FOG TO RSU).
     */
    private void sendRandomF2RMsg() {
        String fogId = getOs().getId().toUpperCase();
        long time = getOs().getSimulationTime();
        long timestampLimit = time + 10000L; // CONSTANT: TIMESTAMP LIMIT
        String order = "EMERGENCY_BRAKE";
        String messageType = "COMMAND";
        // SELECT VEHICLE DESTINATION - CURRENTLY FIXED AS VEH_2
        String vehDest = "VEH_0";
        // FIXED RSU DESTINATION
        String rsuDest = "RSU_0";
        // GENERATE UNIQUE ID FOR DEBUG PURPOSES
        String uniqueId = "F2R-" + time;
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        F2RMsg f2rMsg = new F2RMsg(
                routing,
                uniqueId,
                time,
                timestampLimit,
                messageType,
                fogId,
                rsuDest,
                vehDest,
                order
        );
        getOs().getAdHocModule().sendV2xMessage(f2rMsg);
        String key = fogId + "-" + rsuDest + "-" + f2rMsg.getTimeStamp();
        sentBigArchive.put(key, f2rMsg);
        logInfo("SENT F2R MESSAGE: COMMAND SENT TO VEHICLE [" + vehDest + "] WITH KEY: " + key);
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rx) {
        V2xMessage msg = rx.getMessage();
        if (msg instanceof R2FMsg) {
            R2FMsg r2fMsg = (R2FMsg) msg;
            String rsuSender = r2fMsg.getRsuName().toUpperCase();
            String key = rsuSender + "-" + r2fMsg.getReceiver().toUpperCase() + "-" + r2fMsg.getTimeStamp();
            receivedBigArchive.put(key, r2fMsg);
            logInfo("RECEIVED R2F MESSAGE FROM [" + rsuSender + "] STORED WITH KEY: " + key);
        }
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        logInfo("MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        logInfo("ACKNOWLEDGEMENT RECEIVED: " + ack.toString());
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        logInfo("CAM EVENT TRIGGERED");
    }

    /**
     * PRINTS THE HISTORY OF SENT AND RECEIVED MESSAGES.
     */
    private void printMessageHistory() {
        String fogId = getOs().getId().toUpperCase();
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(fogId).append("] [INFO] RECEIVED BIG ARCHIVE:\n");
        for (Map.Entry<String, R2FMsg> entry : receivedBigArchive.entrySet()) {
            sb.append("KEY: ").append(entry.getKey())
              .append(" -> ").append(entry.getValue()).append("\n");
        }
        sb.append("[").append(fogId).append("] [INFO] SENT BIG ARCHIVE:\n");
        for (Map.Entry<String, F2RMsg> entry : sentBigArchive.entrySet()) {
            sb.append("KEY: ").append(entry.getKey())
              .append(" -> ").append(entry.getValue()).append("\n");
        }
        logInfo(sb.toString());
    }
}