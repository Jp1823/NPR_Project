package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CellModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.DATA;
import org.eclipse.mosaic.rti.TIME;

import pt.uminho.npr.projeto.messages.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class RsuApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private static final long TICK_INTERVAL = 500 * TIME.MILLI_SECOND;
    private static final long CAM_TTL       = 2 * TIME.SECOND;

    private static final int TX_POWER_DBM  = 50;
    private static final double TX_RANGE_M = 140.0;

    private MessageRouting cellRoutingToFog;
    private MessageRouting broadcastRouting;
    
    private final Map<String, CamMessage> seenCams   = new HashMap<>();
    private final Set<String> neighbors = new HashSet<>();
    private String rsuId;
    
    @Override
    public void onStartup() {
        
        // Initialize RSU ID
        rsuId = getOs().getId();
        
        // Enable communication modules
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
            .addRadio()
            .channel(AdHocChannel.CCH)
            .power(TX_POWER_DBM)
            .distance(TX_RANGE_M)
            .create()
        );
        getOs().getCellModule().enable(new CellModuleConfiguration()
            .maxDownlinkBitrate(50 * DATA.MEGABIT)
            .maxUplinkBitrate(50 * DATA.MEGABIT)
        );

        // Initialize Message Routings
        cellRoutingToFog = getOs().getCellModule()
            .createMessageRouting()
            .destination("server_0")
            .topological()
            .build();
        broadcastRouting = getOs().getAdHocModule()
            .createMessageRouting()
            .broadcast()
            .topological()
            .build();
        
        scheduleEvent();
        logInfo("RSU_INITIALIZATION");
    }

    @Override
    public void onShutdown() {
        getOs().getAdHocModule().disable();
        getOs().getCellModule().disable();
        logInfo("RSU_SHUTDOWN");
    }

    @Override
    public void processEvent(Event e) {
        purgeCams();
        scheduleEvent();
    }

    private void purgeCams() {
        long now = getOs().getSimulationTime();
        seenCams.entrySet().removeIf(e ->
            (now - e.getValue().getTimestamp() > CAM_TTL)
        );
    }

    private void scheduleEvent() {
        long next = getOs().getSimulationTime() + TICK_INTERVAL;
        getOs().getEventManager().addEvent(next, this);
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage msg = in.getMessage();

        // If the sender is not the server, add it to the neighbors
        String senderId = msg.getRouting().getSource().getSourceName();
        if (!senderId.equals("server_0")) {
            neighbors.add(senderId);
        }

        if (msg instanceof CamMessage cam && 
            (!seenCams.containsKey(cam.getVehId()) || cam.getId() > seenCams.get(cam.getVehId()).getId())
        ) {
            // Process the CAM message if it is the first seen from the vehicle or if it is more recent
            handleCamReceived(cam);

        } else if (msg instanceof EventACK ack && ack.hasNextHop() && ack.getNextHop().equals(rsuId)) {
            // Process the ACK message if it is for this RSU
            logInfo(String.format(
                "ACK_RECEIVED : UNIQUE_ID: %s", ack.getId()
            ));
            handleAckReceived(ack);
            
        } else if (msg instanceof EventMessage event && event.hasNextHop() && event.getNextHop().equals(rsuId)) {
            // Process the Event message if it is for this RSU
            logInfo(String.format(
                "EVENT_RECEIVED : UNIQUE_ID: %s | EVENT_TYPE: %s | VEHICLE_TARGET: %s", event.getId(), event.getClass().getSimpleName(), event.getTarget()
            ));
            handleEventReceived(event);
        }
    }

    private void handleCamReceived(CamMessage cam) {

        String camVehId = cam.getVehId();
        seenCams.put(camVehId, cam);

        double distance = getOs().getPosition().distanceTo(cam.getPosition());
        boolean reachable = distance < TX_RANGE_M;
        if (reachable) {
            neighbors.add(camVehId);
        } else {
            neighbors.remove(camVehId);
        }
        
        if (cam.getHopsToLive() > 0) {
            forwardCamMessage(cam);
        }
    }

    private void forwardCamMessage(CamMessage cam) {

        // Copy the CAM message to forward it to the fog
        CamMessage camCopy = new CamMessage(
            cellRoutingToFog,
            cam.getId(),
            cam.getVehId(),
            cam.getTimestamp(),
            0, // Time to live is not used in this context
            cam.getPosition(),
            cam.getNeighbors()
        );
        getOs().getCellModule().sendV2xMessage(camCopy);
    }

    private void handleAckReceived(EventACK ack) {

        // If the ACK is expired, do not process it
        if (ack.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "ACK_NOT_FORWARDED : UNIQUE_ID: %s | ACK_EXPIRED",
                ack.getId()
            ));
            return;
        }

        // Remove the last hop (this rsu)
        List<String> checklist = ack.getChecklist();
        checklist.removeLast();

        // Copy the ACK message to forward it
        EventACK ackCopy = new EventACK(
            cellRoutingToFog,
            ack.getId(),
            ack.getTimestamp(),
            ack.getExpiryTimestamp(),
            checklist
        );

        getOs().getCellModule().sendV2xMessage(ackCopy);
        logInfo(String.format(
            "ACK_FORWARDED_TO_FOG : UNIQUE_ID: %s",
            ack.getId()
        ));
    }

    private void handleEventReceived(EventMessage event) {
        
        // If the event is expired, do not process it
        if (event.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "EVENT_NOT_FORWARDED : UNIQUE_ID: %s | VEHICLE_TARGET: %s | EVENT_EXPIRED",
                event.getId(), event.getTarget()
            ));
            return;
        }

        // Select next hop for forwarding
        String target = event.getTarget();
        String nextHop = selectNextHop(target);
    
        // If no next hop is found, do not forward the event
        if (nextHop == null) {
            logInfo(String.format(
                "EVENT_NOT_FORWARDED : UNIQUE_ID: %s | VEHICLE_TARGET: %s | NO_NEXT_HOP_FOUND",
                event.getId(), target
            ));
            return;
        }
    
        // Add the next hop to the forwarding trail
        List<String> forwardingTrail = event.getForwardingTrail();
        forwardingTrail.add(nextHop);

        // Copy the message to forward the event
        EventMessage eventCopy = new AccidentEvent(
            broadcastRouting,
            event.getId(),
            event.getTimestamp(),
            event.getExpiryTimestamp(),
            event.getTarget(),
            forwardingTrail,
            ((AccidentEvent) event).getSeverity()
        );
        
        // Send the event message
        getOs().getAdHocModule().sendV2xMessage(eventCopy);
        logInfo(String.format(
            "EVENT_FORWARDED_TO_VEHICLE : UNIQUE_ID: %s | VEHICLE_TARGET: %s | NEXT_HOP: %s",
            event.getId(), target, nextHop
        ));
    }

    private String selectNextHop(String target) {
        
        // Check if the destination is reachable directly (1 hop)
        if (neighbors.contains(target)) {
            return target;
        }
        
        // If no direct or two-hop neighbor is found, select the closest neighbor to the destination
        return getClosestNeighbor(target);
    }

    private String getClosestNeighbor(String target) {
        
        CamMessage targetCam = seenCams.get(target);
        if (targetCam == null) {
            return null; // No CAM info available for the target
        }

        // Get the current target position
        GeoPoint targetPosition = targetCam.getPosition();

        double minDistance = Double.MAX_VALUE;
        String closestNeighbor = null;

        // Search for the closest neighbor to the target position
        for (String neighbor : neighbors) {
            CamMessage neighborCam = seenCams.get(neighbor);
            if (neighborCam != null) {
                if (neighborCam.getNeighbors().contains(target)) {
                    // If the neighbor is directly connected to the target, return it immediately
                    return neighbor;
                }
                GeoPoint neighborPosition = neighborCam.getPosition();
                double distance = neighborPosition.distanceTo(targetPosition);
                if (distance <= minDistance) {
                    minDistance = distance;
                    closestNeighbor = neighbor;
                }
            }
        }
        return closestNeighbor;
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { /* No action required */ }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { /* No action required */ }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { /* No action required */ }

    private void logInfo(String m) {
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO]  " + m);
    }
}