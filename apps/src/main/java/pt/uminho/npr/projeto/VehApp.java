package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;

import pt.uminho.npr.projeto.messages.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

// Vehicle application for V2X communication and event handling in Eclipse MOSAIC
public final class VehApp extends AbstractApplication<VehicleOperatingSystem>
        implements VehicleApplication, CommunicationApplication {

    // Configuration constants
    private static final long TICK_INTERVAL = 100 * TIME.MILLI_SECOND;
    private static final long CAM_TTL = 2 * TIME.SECOND;
    private static final int  CAM_HOP_TL = 5;
    private static final long CHANGE_ACCEL_DURATION = 1 * TIME.SECOND;

    private static final long   CAM_MIN_TIME = 100  * TIME.MILLI_SECOND;
    private static final long   CAM_MAX_TIME = 1000 * TIME.MILLI_SECOND;
    private static final double HEADING_THRESHOLD = 4.0; // 4 degrees
    private static final double DISTANCE_THRESHOLD = 4.0; // 4 meters
    private static final double SPEED_THRESHOLD = 0.5; // 0.5 m/s
    
    private MessageRouting broadcastRouting;
    private static final int TX_POWER_DBM = 50;
    private static final double TX_RANGE_M = 140.0;
    
    private final Map<String, CamMessage> seenCams = new HashMap<>();
    private final Set<String> neighbors = new HashSet<>();
    private AtomicInteger camIdSeq = new AtomicInteger();
    
    private final Set<Integer> processedEvents = new HashSet<>();
    private long accidentDuration = 30 * TIME.SECOND; // Default accident duration
    private long resumeTimestamp = 0;
    private boolean pendingBrake = false;

    private String vehId;
    private long lastCamTimestamp = 0; // Timestamp of the last CAM sent
    private GeoPoint lastCamPosition = null; // Last position when CAM was sent
    private double lastCamHeading = 0.0; // Last heading in degrees
    private double lastCamSpeed = 0.0; // Last speed in m/s

    @Override
    public void onStartup() {
        // Initialize vehicle ID
        vehId = getOs().getId();

        // Enable ad-hoc communication module
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER_DBM)
                .distance(TX_RANGE_M)
                .create()
        );

        // Initialize broadcast routing
        broadcastRouting = getOs().getAdHocModule()
            .createMessageRouting()
            .broadcast()
            .topological()
            .build();

        scheduleEvent();
        logInfo("VEHICLE_INITIALIZATION");
    }

    @Override
    public void onShutdown() {
        getOs().getAdHocModule().disable();
        logInfo("VEHICLE_SHUTDOWN");
    }

    @Override
    public void onVehicleUpdated(@Nullable org.eclipse.mosaic.lib.objects.vehicle.VehicleData prev,
                                 @Nonnull org.eclipse.mosaic.lib.objects.vehicle.VehicleData cur) {

        // Handle pending events
        if (pendingBrake && resumeTimestamp == 0) {

            // Apply emergency brake
            getOs().changeSpeedWithInterval(0.0, CHANGE_ACCEL_DURATION);
            logInfo("APPLYING EMERGENCY BRAKE FOR " + (accidentDuration / TIME.SECOND) + " SECONDS");
            
            // Schedule resuming speed after the accident duration
            resumeTimestamp = getOs().getSimulationTime() + accidentDuration;
            getOs().getEventManager().addEvent(resumeTimestamp, this);

            pendingBrake = false;
        }
    }

    @Override
    public void processEvent(Event event) {
        long now = getOs().getSimulationTime();

        // Check if it is time to end the accident and resume speed
        if (resumeTimestamp != 0 && now > resumeTimestamp) {
            getOs().resetSpeed();
            logInfo("RESUMING ORIGINAL SPEED");
            resumeTimestamp = 0;
        }
            
        purgeStale(now);
        checkLastCam(now);
        scheduleEvent();
    }
    
    private void purgeStale(long now) {
        // Remove old cam entries
        seenCams.entrySet().removeIf(
            e -> now - e.getValue().getTimestamp() > CAM_TTL
        );
    }

    private void scheduleEvent() {
        // Schedule the next event
        getOs().getEventManager().addEvent(
            getOs().getSimulationTime() + TICK_INTERVAL,
            this
        );
    }
        
    private void checkLastCam(long now) {

        // Calculate elapsed time since last CAM (T)
        long elapsedTime = now - lastCamTimestamp;
        
        // Get current vehicle state
        GeoPoint currentPosition = getOs().getPosition();
        double currentHeading = getOs().getVehicleData().getHeading();
        double currentSpeed = getOs().getVehicleData().getSpeed();
        
        if (elapsedTime >= CAM_MAX_TIME) {
            // T >= TMax: Send CAM regardless of changes
            sendCam(now, currentPosition, currentHeading, currentSpeed);
            return;    
        }

        // Initialize last CAM position if not set
        if (lastCamPosition == null) {
            lastCamPosition = getOs().getPosition();
        }

        // Calculate major changes
        boolean hasPositionChanged = lastCamPosition.distanceTo(currentPosition) > DISTANCE_THRESHOLD;
        boolean hasHeadingChanged = Math.abs(currentHeading - lastCamHeading) > HEADING_THRESHOLD;
        boolean hasSpeedChanged = Math.abs(currentSpeed - lastCamSpeed) > SPEED_THRESHOLD;

        if (elapsedTime >= CAM_MIN_TIME &&
            (hasHeadingChanged || hasPositionChanged || hasSpeedChanged)
        ) {
            // T >= TMin AND any major change: Send CAM       
            sendCam(now, currentPosition, currentHeading, currentSpeed);
        }
    }

    private void sendCam(long now, GeoPoint position, double heading, double speed) {
        // Create a new CAM message and send it
        CamMessage v2v = new CamMessage(
            broadcastRouting,
            camIdSeq.getAndIncrement(),
            vehId,
            now,
            CAM_HOP_TL,
            position,
            neighbors
        );
        getOs().getAdHocModule().sendV2xMessage(v2v);

        // Update last CAM state
        lastCamTimestamp = now;
        lastCamPosition = position;
        lastCamHeading = heading;
        lastCamSpeed = speed;
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage msg = in.getMessage();

        // If the sender is a vehicle, add it to the neighbors   
        String senderId = msg.getRouting().getSource().getSourceName();
        if (senderId.contains("veh")) {
            neighbors.add(senderId);
        }

        if (msg instanceof CamMessage cam && 
            (!seenCams.containsKey(cam.getVehId()) || cam.getId() > seenCams.get(cam.getVehId()).getId())
        ) {
            // Process the CAM message if it is the first seen from the vehicle or if it is more recent
            handleCamReceived(cam);

        } else if (msg instanceof EventMessage event && event.hasNextHop() && 
                   event.getNextHop().equals(vehId)  && processedEvents.add(event.getId())
        ) {
            // Process event message if it is directed to this vehicle and not already processed
            logInfo("EVENT MESSAGE RECEIVED: EVENT_ID =" + event.getId() +
                    " | VEHICLE_TARGET = " + event.getTarget() +
                    " | EVENT_TYPE = " + event.getSimpleClassName());
            handleEventReceived(event);

        } else if (msg instanceof EventACK ack && ack.hasNextHop() && ack.getNextHop().equals(vehId)) {
            // Process the ack if it is directed to this vehicle
            logInfo("EVENT ACK RECEIVED:" +
                    " | ACK_ID = " + ack.getId() +
                    " | NEXT_HOP = " + ack.getNextHop());
            handleAckReceived(ack);
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

        // Forward message if TTL allows
        if (cam.getHopsToLive() > 0) {
            forwardCamMessage(cam);
        }
    }    

    private void forwardCamMessage(CamMessage cam) {
        CamMessage fwd = new CamMessage(
            broadcastRouting,
            cam.getId(),
            cam.getVehId(),
            cam.getTimestamp(),
            cam.getHopsToLive() - 1,
            cam.getPosition(),
            cam.getNeighbors()
        );
        getOs().getAdHocModule().sendV2xMessage(fwd);
    }

    private void handleAckReceived(EventACK ack) {

        // If the ACK is expired, do not process it
        if (ack.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "ACK_NOT_PROCESSED : UNIQUE_ID: %d | ACK_EXPIRED",
                ack.getId()
            ));
            return;
        }

        // Remove the last hop (this vehicle)
        List<String> checklist = ack.getChecklist();
        checklist.removeLast(); 
        logInfo(vehId + " | check list SIZE: " + checklist.size());

        // Copy the ACK message to forward it
        EventACK ackCopy = new EventACK(
            broadcastRouting,
            ack.getId(),
            ack.getTimestamp(),
            ack.getExpiryTimestamp(),
            checklist
        );

        // Send the ACK message copy
        getOs().getAdHocModule().sendV2xMessage(ackCopy);
        logInfo(String.format(
            "ACK SENT: EVENT_ID = %d | NEXT_HOP = %s", 
            ackCopy.getId(), ackCopy.getNextHop()
        ));
    }

    private void handleEventReceived(EventMessage event) {
        
        if(event.getTarget().equals(vehId)) {
            processEvent(event);
        } else {
            forwardEvent(event);
        }
    }

    private void processEvent(EventMessage event) {

        // If the event is expired, do not process it
        if (event.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "EVENT_NOT_PROCESSED : UNIQUE_ID: %d | VEHICLE_TARGET: %s | EVENT_EXPIRED",
                event.getId(), event.getTarget()
            ));
            return;
        }

        logInfo(String.format(
            "FINAL TARGET REACHED | PROCESSING EVENT : UNIQUE_ID = %d", 
            event.getId()
        ));
        if(event instanceof AccidentEvent aEvent) {
            logInfo("ACCIDENT EVENT RECEIVED: SCHEDULING EMERGENCY BRAKE");
            pendingBrake = true;
            switch (aEvent.getSeverity()) {
                case 0: // Minor accident
                    accidentDuration = 15 * TIME.SECOND;
                    break;
                case 1: // Moderate accident
                    accidentDuration = 30 * TIME.SECOND;
                    break;
                case 2: // Severe accident
                    accidentDuration = 60 * TIME.SECOND;
                    break;
                default:
                    logInfo("UNKNOWN SEVERITY LEVEL: " + aEvent.getSeverity());
                    accidentDuration = 30 * TIME.SECOND; // Default to 30 seconds
                    break;
            }
        } else {
            logInfo("UNKNOWN EVENT TYPE: " + event.getSimpleClassName());
        }

        // Send acknowledgement back to the source
        sendAcknowledgement(event);
    }

    private void sendAcknowledgement(EventMessage event) {
        
        long now = getOs().getSimulationTime();

        // Build reverse path for ACK
        List<String> forwardingTrail = event.getForwardingTrail();
        forwardingTrail.removeLast(); // Remove the last hop (this vehicle)

        EventACK ack = new EventACK(
            broadcastRouting,
            event.getId(),
            now,
            now + (10_000 * TIME.MILLI_SECOND),
            forwardingTrail
        );

        getOs().getAdHocModule().sendV2xMessage(ack);
        logInfo(String.format(
            "ACK SENT: EVENT_ID = %d | NEXT_HOP = %s", 
            ack.getId(), ack.getNextHop()
        ));
    }

    private void forwardEvent(EventMessage event) {

        // If the event is expired, do not forward it
        if (event.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "EVENT_NOT_FORWARDED : UNIQUE_ID: %d | VEHICLE_TARGET: %s | EVENT_EXPIRED",
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
        logInfo(vehId + " | FORWARDING TRAIL SIZE: " + forwardingTrail.size());

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
            "EVENT_FORWARDED_TO_VEHICLE : UNIQUE_ID: %d | VEHICLE_TARGET: %s | NEXT_HOP: %s",
            event.getId(), target, nextHop
        ));
    }

    private String selectNextHop(String target) {

        // Check if the destination is reachable directly (1 hop)
        if (neighbors.contains(target)) {
            return target;
        }

        // If no direct neighbor is found, select the closest neighbor to the destination
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
    
    // Logging
    private void logInfo(String msg) {
        getLog().infoSimTime(this, "[" + vehId + "] [info] " + msg);
    }
}