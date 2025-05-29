package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CellModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.DATA;
import org.eclipse.mosaic.rti.TIME;

import pt.uminho.npr.projeto.messages.*;
import pt.uminho.npr.projeto.records.NodeRecord;

import java.util.*;
import java.util.stream.Collectors;

public final class RsuApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private static final long   CLEAN_MS        = 1000 * TIME.MILLI_SECOND;
    private static final int    TX_POWER_DBM    = 23;
    private static final double TX_RANGE_M      = 120.0;
    private static final long   NEIGHBOR_TTL_MS = 1000 * TIME.MILLI_SECOND;

    private static final double W_TWO_HOP      = 50.0;
    private static final double W_PROXIMITY    = 30.0;
    private static final double W_CONNECTIVITY = 10.0;
    private static final double W_STABILITY    =  5.0;
    private static final double W_FAIRNESS     =  5.0;

    private final MessageRouting cellRoutingToFog = getOs().getCellModule()
        .createMessageRouting()
        .destination("server_0")
        .topological()
        .build();

    private final MessageRouting broadcastRouting = getOs().getAdHocModule()
        .createMessageRouting()
        .broadcast()
        .topological()
        .build();
    
    private final Map<String, NodeRecord> neighbors = new HashMap<>();
    private final Map<String, Integer> fairnessCount = new HashMap<>();
    private final Set<Integer> seenCams = new HashSet<>();
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
        purgeNeighbors();
        scheduleEvent();
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage msg = in.getMessage();
        
        if (msg instanceof CamMessage cam && seenCams.add(cam.getId())) {
            // logInfo("PROCESSING VEHICLE TO VEHICLE MESSAGE ID = " + v2v.getMessageId());
            handleCamReceived(cam);

        } else if (msg instanceof EventACK ack && ack.getNextHop().equals(rsuId)) {
            // Process the ACK message if it is for this RSU
            logInfo(String.format(
                "ACK_RECEIVED : UNIQUE_ID: %s", ack.getId()
            ));
            handleAckReceived(ack);
            
        } else if (msg instanceof EventMessage event && event.getNextHop() == null) {
            // If the next hop is null, it means the event came from the fog
            logInfo(String.format(
                "EVENT_RECEIVED : UNIQUE_ID: %s | EVENT_TYPE: %s | VEHICLE_TARGET: %s", event.getId(), event.getClass().getSimpleName(), event.getTarget()
            ));
            handleEventReceived(event);
        }
    }

    private void handleCamReceived(CamMessage cam) {

        String vid = cam.getVehId();
        double d   = getOs().getPosition().distanceTo(cam.getPosition());
        boolean reachable = d <= TX_RANGE_M;
        List<String> reachableNeighbors = new ArrayList<>(cam.getNeighborsGraph().keySet());
        List<String> directNeighbors = reachableNeighbors.stream()
            .filter(id -> {
                NodeRecord nr = cam.getNeighborsGraph().get(id);
                return nr != null && nr.getDistanceFromVehicle() <= TX_RANGE_M;
            })
            .toList();

        NodeRecord rec = new NodeRecord(
            d,
            d,
            reachable,
            reachableNeighbors,
            directNeighbors,
            cam.getTimeStamp()
        );
        neighbors.put(vid, rec);
        
        CamMessage camCopy = new CamMessage(
            cellRoutingToFog,
            cam.getId(),
            cam.getVehId(),
            cam.getTimeStamp(),
            cam.getPosition(),
            cam.getHeading(),
            cam.getSpeed(),
            cam.getAcceleration(),
            cam.isBrakeLightOn(),
            cam.isLeftTurnSignalOn(),
            cam.isRightTurnSignalOn(),
            cam.getTimeToLive(),
            cam.getNeighborsGraph()
        );
        getOs().getCellModule().sendV2xMessage(camCopy);
        // logInfo("RTF MESSAGE ID = " + rtf.getUniqueId() + " SENT TO FOG ID = " + fogId);
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

        // Get the target vehicle from the event
        String target = event.getTarget();
    
        // If the target vehicle is not in the neighbor list, try to select a next hop
        String nextHop = null;
        if (neighbors.get(target) != null) {
            nextHop = target;
        } else {
            nextHop = selectNextHop(target);
        }
    
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

    private String selectNextHop(String dst) {
        List<String> direct = neighbors.entrySet().stream()
            .filter(e -> e.getValue().isReachableToRsu())
            .map(Map.Entry::getKey)
            .toList();
        if (direct.isEmpty()) return null;
    
        Set<String> twoHop = direct.stream()
            .filter(n -> neighbors.get(n).getReachableNeighbors().contains(dst))
            .collect(Collectors.toSet());
    
        List<String> candidates = twoHop.isEmpty() ? direct : new ArrayList<>(twoHop);

        double maxDegree     = candidates.stream()
            .mapToInt(n -> neighbors.get(n).getReachableNeighbors().size())
            .max().orElse(1);
        int maxFairness      = fairnessCount.values().stream().mapToInt(i -> i).max().orElse(1);
        long now             = getOs().getSimulationTime();

        double maxStability  = candidates.stream()
            .mapToDouble(n -> now - neighbors.get(n).getCreationTimestamp())
            .max().orElse(1.0);
    
        String best     = null;
        double bestScore= Double.NEGATIVE_INFINITY;
        for (String cand : candidates) {
            NodeRecord rec = neighbors.get(cand);
    
            double scoreTwoHop = twoHop.contains(cand) ? 1.0 : 0.0;

            double scoreProx   = 1.0 / rec.getDistanceFromVehicle();

            double scoreConn   = rec.getReachableNeighbors().size() / maxDegree;

            long   age         = now - rec.getCreationTimestamp();
            double scoreStab   = age / maxStability;

            int    usedTimes   = fairnessCount.getOrDefault(cand, 0);
            double scoreFair   = 1.0 - (usedTimes / (double) maxFairness);
    
            double score = W_TWO_HOP      * scoreTwoHop
                         + W_PROXIMITY    * scoreProx
                         + W_CONNECTIVITY * scoreConn
                         + W_STABILITY    * scoreStab
                         + W_FAIRNESS     * scoreFair;
    
            if (score > bestScore) {
                bestScore = score;
                best      = cand;
            }
        }
    
        fairnessCount.merge(best, 1, Integer::sum);
        return best;
    }
    
    private void purgeNeighbors() {
        long now = getOs().getSimulationTime();
        neighbors.entrySet().removeIf(e ->
            (now - e.getValue().getCreationTimestamp() > NEIGHBOR_TTL_MS)
            || !e.getValue().isReachableToRsu()
        );
    }

    private void scheduleEvent() {
        long next = getOs().getSimulationTime() + CLEAN_MS;
        getOs().getEventManager().addEvent(next, this);
    }

    /*
    private void printNeighborEntries() {
        logInfo("NEIGHBOR_GRAPH : SIZE = " + neighbors.size());
        neighbors.keySet().stream().sorted().forEach(key -> {
            NodeRecord rec = neighbors.get(key);
            logInfo("NEIGHBOR_ENTRY : VEHICLE_ID: " + key +
                    " | DISTANCE_FROM_RSU: " + String.format("%.2f", rec.getDistanceFromVehicle()) +
                    " | REACHABLE_TO_RSU: " + rec.isReachableToRsu() +
                    " | REACHABLE_NEIGHBORS: " + rec.getReachableNeighbors().size() +
                    " | DIRECT_NEIGHBORS: " + rec.getDirectNeighbors().size() +
                    " | CREATION_TIMESTAMP: " + rec.getCreationTimestamp());
        });
    }
    */

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { /* No action required */ }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { /* No action required */ }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { /* No action required */ }

    private void logInfo(String m) {
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO]  " + m);
    }
}