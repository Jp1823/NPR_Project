package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.VehApp.VehicleGraph.NodeRecord;
import pt.uminho.npr.tutorial.Messages.R2VMsg;
import pt.uminho.npr.tutorial.Messages.V2RACK;
import pt.uminho.npr.tutorial.Messages.V2VExt;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * VEHAPP IMPLEMENTATION FOR V2X COMMUNICATION, NEIGHBOR MANAGEMENT, AND MESSAGE FORWARDING.
 * THIS VERSION INTEGRATES THE REQUIRED NEIGHBOR INFORMATION (DISTANCE TO VEHICLE, NEIGHBOR'S RSU DISTANCE,
 * RSU REACHABILITY, AND NEIGHBOR LIST) INTO A SINGLE, DEDICATED VEHICLEGRAPH CLASS, RESULTING IN A ROBUST,
 * MODULAR, AND SIMPLIFIED DESIGN.
 */
public class VehApp extends AbstractApplication<VehicleOperatingSystem>
        implements VehicleApplication, CommunicationApplication {

    // CONSTANTS
    private final long MSG_DELAY = 100 * TIME.MILLI_SECOND;
    private final int TX_POWER = 50;
    private final double TX_RANGE = 100.0;
    private final double RSU_RANGE = 100.0;
    private final long CLEAN_THRESHOLD = 1000 * TIME.MILLI_SECOND;

    // VEHICLE STATE VARIABLES
    private boolean startSending = false;
    private double vehHeading;
    private double vehSpeed;
    private int vehLane;

    // STATIC RSU INFORMATION (E.G., A FIXED RSU)
    private final Map<String, GeoPoint> staticRsus = new HashMap<>();

    // VEHICLEGRAPH: MANAGES ALL NEIGHBOR RECORDS AND ASSOCIATED GRAPH INFORMATION
    private final VehicleGraph vehicleGraph = new VehicleGraph();

    private static final AtomicInteger v2rCounter = new AtomicInteger(0);

    /**
     * VEHICLEGRAPH CLASS.
     * THIS CLASS CENTRALIZES THE MANAGEMENT OF NEIGHBOR INFORMATION INCLUDING:
     * - DISTANCE FROM THE CURRENT VEHICLE TO THE NEIGHBOR,
     * - NEIGHBOR'S DISTANCE TO THE NEAREST RSU,
     * - WHETHER THE NEIGHBOR HAS A REACHABLE RSU,
     * - THE LIST OF NEIGHBORS OF THE NEIGHBOR,
     * - AND THE TIMESTAMP OF THE LAST UPDATE.
     */
    public static class VehicleGraph {

        // MAP: VEHICLE ID -> NODE RECORD
        private final Map<String, NodeRecord> nodes = new HashMap<>();

        /**
         * NODE RECORD.
         */
        public static class NodeRecord {
            private final double distanceToVehicle;
            private final double rsuDistance;
            private final boolean rsuReachable;
            private final List<String> neighborList;
            private final long timestamp;

            public NodeRecord(double distanceToVehicle, double rsuDistance, boolean rsuReachable,
                              List<String> neighborList, long timestamp) {
                this.distanceToVehicle = distanceToVehicle;
                this.rsuDistance = rsuDistance;
                this.rsuReachable = rsuReachable;
                this.neighborList = new ArrayList<>(neighborList);
                this.timestamp = timestamp;
            }

            public double getDistanceToVehicle() {
                return distanceToVehicle;
            }

            public double getRsuDistance() {
                return rsuDistance;
            }

            public boolean isRsuReachable() {
                return rsuReachable;
            }

            public List<String> getNeighborList() {
                return Collections.unmodifiableList(neighborList);
            }

            public long getTimestamp() {
                return timestamp;
            }

            @Override
            public String toString() {
                return String.format("[DISTANCE: %.2fM] [RSU DIST: %.2fM] [RSU REACHABLE: %s] [NEIGHBOR LIST: %s]",
                        distanceToVehicle, rsuDistance, rsuReachable ? "YES" : "NO",
                        neighborList.toString().toUpperCase());
            }
        }

        /**
         * ADDS OR UPDATES A NODE RECORD FOR A GIVEN VEHICLE.
         *
         * @param vehicleId THE VEHICLE ID.
         * @param record    THE NODE RECORD.
         */
        public void addOrUpdateNode(String vehicleId, NodeRecord record) {
            nodes.put(vehicleId, record);
        }

        /**
         * RETURNS AN UNMODIFIABLE SNAPSHOT OF CURRENT NODE RECORDS.
         *
         * @return SNAPSHOT OF NODE RECORDS.
         */
        public Map<String, NodeRecord> getNodesSnapshot() {
            return Collections.unmodifiableMap(new HashMap<>(nodes));
        }

        /**
         * REMOVES NODE RECORDS THAT ARE STALE BASED ON THE SPECIFIED THRESHOLD.
         *
         * @param currentTime CURRENT SIMULATION TIME.
         * @param threshold   TIME THRESHOLD.
         */
        public void removeStaleNodes(long currentTime, long threshold) {
            Iterator<Map.Entry<String, NodeRecord>> it = nodes.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, NodeRecord> entry = it.next();
                if (currentTime - entry.getValue().getTimestamp() > threshold) {
                    it.remove();
                }
            }
        }

        /**
         * CALCULATES THE MINIMUM HOP COUNT BETWEEN TWO VEHICLES USING BFS.
         * THE ADJACENCY IS BASED ON THE NEIGHBOR LIST OF EACH NODE.
         *
         * @param start       THE STARTING VEHICLE.
         * @param destination THE DESTINATION VEHICLE.
         * @return MINIMUM NUMBER OF HOPS OR Integer.MAX_VALUE IF NOT REACHABLE.
         */
        public int getMinHopCount(String start, String destination) {
            if (start.equalsIgnoreCase(destination)) return 0;
            Queue<String> queue = new LinkedList<>();
            Map<String, Integer> distanceMap = new HashMap<>();
            queue.offer(start);
            distanceMap.put(start, 0);
            while (!queue.isEmpty()) {
                String current = queue.poll();
                int hops = distanceMap.get(current);
                if (current.equalsIgnoreCase(destination)) {
                    return hops;
                }
                NodeRecord record = nodes.get(current);
                if (record != null) {
                    for (String neighbor : record.getNeighborList()) {
                        if (!distanceMap.containsKey(neighbor)) {
                            distanceMap.put(neighbor, hops + 1);
                            queue.offer(neighbor);
                        }
                    }
                }
            }
            return Integer.MAX_VALUE;
        }
    }

    /**
     * LOGS A MESSAGE AT THE INFO LEVEL (IMPORTANT EVENTS, INTERNAL STRUCTURE PRINTS, ETC.).
     *
     * @param message THE MESSAGE TO LOG.
     */
    private void logInfo(String message) {
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [INFO] " + message.toUpperCase());
    }

    /**
     * LOGS A MESSAGE AT THE DEBUG LEVEL (DETAILED PROCESSING INFORMATION).
     *
     * @param message THE MESSAGE TO LOG.
     */
    private void logDebug(String message) {
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [DEBUG] " + message.toUpperCase());
    }

    /**
     * ADDS STATIC RSUS TO THE VEHICLE GRAPH.
     * THIS METHOD CALCULATES THE DISTANCE FROM THE CURRENT VEHICLE POSITION TO EACH STATIC RSU,
     * DETERMINES REACHABILITY, AND INSERTS/UPDATES THE RECORD IN THE VEHICLE GRAPH.
     */
    private void addStaticRsusToGraph() {
        GeoPoint currentPos = getOs().getPosition();
        long currentTime = getOs().getSimulationTime();
        for (Map.Entry<String, GeoPoint> entry : staticRsus.entrySet()) {
            String rsuId = entry.getKey().toUpperCase();
            GeoPoint rsuPos = entry.getValue();
            double distance = computeDistance(currentPos, rsuPos);
            boolean reachable = distance <= RSU_RANGE;
            // CREATE A NODE RECORD FOR THE STATIC RSU
            VehicleGraph.NodeRecord rsuRecord = new VehicleGraph.NodeRecord(
                    distance, // DISTANCE FROM VEHICLE TO RSU
                    distance, // RSU DISTANCE (CAN BE SAME AS VEHICLE DISTANCE)
                    reachable,
                    new ArrayList<>(), // EMPTY NEIGHBOR LIST FOR STATIC RSU
                    currentTime
            );
            vehicleGraph.addOrUpdateNode(rsuId, rsuRecord);
            logDebug("ADDED STATIC RSU TO GRAPH: " + rsuId + " WITH DISTANCE: " + String.format("%.2f", distance));
        }
    }

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
        // INITIALIZE STATIC RSU INFORMATION
        staticRsus.put("RSU_0", GeoPoint.latLon(52.454540, 13.306261, 0));
        // ADD STATIC RSUS TO VEHICLE GRAPH
        addStaticRsusToGraph();
        logInfo("VEHICLE APP STARTUP");
        // SCHEDULE FIRST EVENT
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        printNeighborGraph();
        logInfo("VEHICLE APP SHUTDOWN");
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
            logInfo("VEHICLE APP READY TO SEND NEIGHBOR INFORMATION");
        }
    }

    @Override
    public void processEvent(Event event) {
        long currentTime = getOs().getSimulationTime();
        if (startSending) {
            // UPDATE STATIC RSUS IN THE GRAPH BEFORE SENDING V2VEXT MESSAGE
            addStaticRsusToGraph();
            // REMOVE STALE RECORDS FROM THE VEHICLE GRAPH
            vehicleGraph.removeStaleNodes(currentTime, CLEAN_THRESHOLD);
            sendV2VExtMsg();
        }
        // ALWAYS PRINT CURRENT VEHICLE GRAPH
        printNeighborGraph();
        // SCHEDULE NEXT EVENT
        getOs().getEventManager().addEvent(currentTime + MSG_DELAY, this);
    }

    /**
     * SENDS A V2V EXTENDED MESSAGE CONTAINING NEIGHBOR INFORMATION.
     */
    private void sendV2VExtMsg() {
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        long currentTime = getOs().getSimulationTime();
        // OBTAIN LIST OF NEIGHBOR IDS FROM THE VEHICLE GRAPH
        List<String> neighborList = new ArrayList<>(vehicleGraph.getNodesSnapshot().keySet());
        V2VExt extMsg = new V2VExt(
                routing,
                currentTime,
                getOs().getId().toUpperCase(),
                getOs().getPosition(),
                vehHeading,
                vehSpeed,
                vehLane,
                neighborList
        );
        getOs().getAdHocModule().sendV2xMessage(extMsg);
        logDebug("SENT V2VEXT: " + extMsg.toString() + " AT TIME " + currentTime);
    }

    /**
     * COMPUTES THE DISTANCE BETWEEN TWO GEOPOINTS USING THE HAVERSINE FORMULA.
     *
     * @param p1 FIRST GEOPOINT.
     * @param p2 SECOND GEOPOINT.
     * @return DISTANCE IN METERS.
     */
    private double computeDistance(GeoPoint p1, GeoPoint p2) {
        double earthRadius = 6371000; // METERS
        double dLat = Math.toRadians(p2.getLatitude() - p1.getLatitude());
        double dLon = Math.toRadians(p2.getLongitude() - p1.getLongitude());
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(p1.getLatitude())) *
                   Math.cos(Math.toRadians(p2.getLatitude())) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return earthRadius * c;
    }

    /**
     * DETERMINES THE NEAREST RSU FOR A GIVEN POSITION.
     *
     * @param pos          GEOPOINT OF THE TARGET.
     * @param msgTimestamp MESSAGE TIMESTAMP.
     * @return A NODE RECORD CONTAINING THE NEAREST RSU INFORMATION.
     */
    private VehicleGraph.NodeRecord computeNearestRsu(GeoPoint pos, long msgTimestamp) {
        String bestRsu = "";
        double minDistance = Double.MAX_VALUE;
        for (Map.Entry<String, GeoPoint> entry : staticRsus.entrySet()) {
            double distance = computeDistance(pos, entry.getValue());
            if (distance < minDistance) {
                minDistance = distance;
                bestRsu = entry.getKey();
            }
        }
        boolean reachable = minDistance <= RSU_RANGE;
        logDebug("COMPUTED NEAREST RSU: " + bestRsu + " WITH DISTANCE: " + String.format("%.2f", minDistance)
                 + "M, REACHABLE: " + (reachable ? "YES" : "NO") + ", MSG TIMESTAMP: " + msgTimestamp);
        return new VehicleGraph.NodeRecord(0, minDistance, reachable, new ArrayList<>(), msgTimestamp);
    }

    /**
     * PRINTS THE CURRENT VEHICLEGRAPH (ALL NODE RECORDS).
     */
    private void printNeighborGraph() {
        StringBuilder sb = new StringBuilder();
        Map<String, VehicleGraph.NodeRecord> snapshot = vehicleGraph.getNodesSnapshot();
        for (Map.Entry<String, VehicleGraph.NodeRecord> entry : snapshot.entrySet()) {
            sb.append(entry.getKey().toUpperCase()).append(" -> ").append(entry.getValue().toString()).append("\n");
        }
        logInfo("[VEHICLEGRAPH] \n" + sb.toString());
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rcvMsg) {
        V2xMessage msg = rcvMsg.getMessage();
        if (msg instanceof V2VExt) {
            V2VExt extMsg = (V2VExt) msg;
            String senderId = extMsg.getSenderName().toUpperCase();
            long msgTimestamp = extMsg.getTimeStamp();
            double distanceToSender = computeDistance(getOs().getPosition(), extMsg.getSenderPos());
            // COMPUTE RSU INFORMATION FOR THE SENDER
            VehicleGraph.NodeRecord rsuInfo = computeNearestRsu(extMsg.getSenderPos(), msgTimestamp);
            // CREATE A NODE RECORD FOR THE NEIGHBOR WITH THE REQUIRED INFORMATION:
            // - DISTANCE FROM CURRENT VEHICLE TO THE NEIGHBOR
            // - NEIGHBOR'S RSU DISTANCE
            // - RSU REACHABILITY
            // - NEIGHBOR LIST OBTAINED FROM THE V2VEXT MESSAGE
            VehicleGraph.NodeRecord nodeRecord = new VehicleGraph.NodeRecord(
                    distanceToSender,
                    rsuInfo.getRsuDistance(),
                    rsuInfo.isRsuReachable(),
                    extMsg.getNeighborList(),
                    msgTimestamp
            );
            vehicleGraph.addOrUpdateNode(senderId, nodeRecord);
            logDebug("UPDATED VEHICLEGRAPH NODE FOR " + senderId + " WITH RECORD: " + nodeRecord.toString());
        } else if (msg instanceof R2VMsg) {
            R2VMsg r2vMsg = (R2VMsg) msg;
            String currentVehId = getOs().getId().toUpperCase();
            // IF THE CURRENT VEHICLE IS NOT THE DESIGNATED NEXT HOP, DROP THE MESSAGE IMMEDIATELY
            if (!r2vMsg.getNextHop().equalsIgnoreCase(currentVehId)) {
                logDebug("CURRENT VEHICLE (" + currentVehId + ") IS NOT THE NEXT HOP (" 
                         + r2vMsg.getNextHop().toUpperCase() + "); DROPPING MESSAGE");
                return;
            }
            // IF CURRENT VEHICLE IS THE FINAL DESTINATION, DELIVER THE MESSAGE AND TERMINATE THE CYCLE
            if (r2vMsg.getVehDestination().equalsIgnoreCase(currentVehId)) {
                logInfo("FINAL DELIVERY R2V MESSAGE RECEIVED: " + r2vMsg.toString());
                handleUpstreamAck(r2vMsg);
                return;
            }
            // IF CURRENT VEHICLE IS ALREADY PRESENT IN THE FORWARDING TRAIL, DROP THE MESSAGE TO AVOID LOOPS
            if (r2vMsg.getForwardingTrail().contains(currentVehId)) {
                logDebug("MESSAGE ALREADY PASSED THROUGH " + currentVehId + "; DROPPING MESSAGE");
                return;
            }
            // EXTRACT THE RSU DESTINATION FROM THE ORIGINAL R2V MESSAGE (RSU SOURCE FIELD)
            String rsuDestination = r2vMsg.getRsuSource();
            // IF THE RSU DESTINATION IS DIRECTLY REACHABLE FROM THE CURRENT VEHICLE, PERFORM DIRECT FORWARDING
            if (vehicleGraph.getNodesSnapshot().containsKey(rsuDestination.toUpperCase())) {
                List<String> updatedTrail = new ArrayList<>(r2vMsg.getForwardingTrail());
                updatedTrail.add(currentVehId);
                MessageRouting newRouting = getOs().getAdHocModule().createMessageRouting()
                    .viaChannel(AdHocChannel.CCH)
                    .topoBroadCast();
                R2VMsg directR2vMsg = new R2VMsg(
                    newRouting,
                    r2vMsg.getUniqueId(),
                    r2vMsg.getTimeStamp(),
                    r2vMsg.getTimestampLimit(),
                    r2vMsg.getRsuSource(), // RSU SOURCE
                    r2vMsg.getVehDestination(),
                    r2vMsg.getVehDestination(), // DIRECT FORWARDING: SET NEXT HOP TO RSU DESTINATION
                    r2vMsg.getOrder(),
                    updatedTrail
                );
                getOs().getAdHocModule().sendV2xMessage(directR2vMsg);
                logInfo("DIRECT R2V MESSAGE FORWARDED TO RSU DESTINATION " + r2vMsg.getRsuSource() +
                        " WITH UPDATED TRAIL: " + updatedTrail.toString());
                return;
            }
            // OTHERWISE, PERFORM MULTIHOP FORWARDING DECISION TOWARD THE RSU
            int bestHops = Integer.MAX_VALUE;
            double bestDistance = Double.MAX_VALUE;
            String bestCandidate = null;
            Map<String, NodeRecord> nodesSnapshot = vehicleGraph.getNodesSnapshot();
            // ITERATE OVER THE VEHICLE GRAPH TO DETERMINE THE BEST FORWARDING PATH TOWARD THE RSU
            for (Map.Entry<String, NodeRecord> entry : nodesSnapshot.entrySet()) {
                String candidate = entry.getKey();
                int hops = vehicleGraph.getMinHopCount(candidate, rsuDestination);
                if (hops == Integer.MAX_VALUE) continue;
                double candidateRsuDist = entry.getValue().getRsuDistance();
                if (hops < bestHops || (hops == bestHops && candidateRsuDist < bestDistance)) {
                    bestHops = hops;
                    bestDistance = candidateRsuDist;
                    bestCandidate = candidate;
                }
            }
            // IF NO SUITABLE CANDIDATE IS FOUND, DROP THE MESSAGE
            if (bestCandidate == null) {
                logInfo("NO CANDIDATE FOUND TO FORWARD R2V MESSAGE TO RSU DESTINATION " + rsuDestination);
                return;
            }
            // UPDATE THE FORWARDING TRAIL AND FORWARD THE R2V MESSAGE TO THE SELECTED BEST CANDIDATE
            List<String> updatedTrail = new ArrayList<>(r2vMsg.getForwardingTrail());
            updatedTrail.add(currentVehId);
            MessageRouting newRouting = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
            R2VMsg newR2vMsg = new R2VMsg(
                newRouting,
                r2vMsg.getUniqueId(),
                r2vMsg.getTimeStamp(),
                r2vMsg.getTimestampLimit(),
                r2vMsg.getRsuSource(), // RSU SOURCE REMAINS UNCHANGED
                r2vMsg.getVehDestination(),
                bestCandidate, // NEXT HOP IS THE BEST CANDIDATE FOR REACHING THE RSU
                r2vMsg.getOrder(),
                updatedTrail
            );
            getOs().getAdHocModule().sendV2xMessage(newR2vMsg);
            logInfo("R2V MESSAGE FORWARDED TO " + bestCandidate + " WITH UPDATED TRAIL: " + updatedTrail.toString());
            return;
        } else if (msg instanceof V2RACK) {
            V2RACK v2rackMsg = (V2RACK) msg;
            String currentVehId = getOs().getId().toUpperCase();
            // IF CURRENT VEHICLE IS NOT THE DESIGNATED NEXT HOP, DROP THE MESSAGE IMMEDIATELY
            if (!v2rackMsg.getNextHop().equalsIgnoreCase(currentVehId)) {
                logDebug("CURRENT VEHICLE (" + currentVehId + ") IS NOT THE NEXT HOP (" 
                         + v2rackMsg.getNextHop().toUpperCase() + "); DROPPING V2RACK MESSAGE");
                return;
            }
            // IF THE CHECKLIST IS EMPTY, THIS MEANS THE UPSTREAM PATH HAS BEEN COMPLETED
            // FOR DIRECT DELIVERY TO THE RSU DESTINATION
            if (v2rackMsg.getChecklist().isEmpty()) {
                MessageRouting newRouting = getOs().getAdHocModule().createMessageRouting()
                    .viaChannel(AdHocChannel.CCH)
                    .topoBroadCast();
                V2RACK directV2rackMsg = new V2RACK(
                    newRouting,
                    v2rackMsg.getUniqueId(),
                    v2rackMsg.getOriginalMsgId(),
                    v2rackMsg.getTimeStamp(),
                    v2rackMsg.getTimestampLimit(),
                    v2rackMsg.getVehicleId(),
                    v2rackMsg.getRsuDestination(),
                    v2rackMsg.getRsuDestination(), // DIRECT NEXT HOP IS THE RSU DESTINATION
                    v2rackMsg.getChecklist() // CHECKLIST REMAINS EMPTY
                );
                getOs().getAdHocModule().sendV2xMessage(directV2rackMsg);
                logInfo("DIRECT V2RACK MESSAGE FORWARDED TO RSU DESTINATION " + v2rackMsg.getRsuDestination() +
                        " WITH EMPTY CHECKLIST");
                return;
            }
            // OTHERWISE, UPDATE THE CHECKLIST BY REMOVING THE CURRENT NEXT HOP
            List<String> updatedChecklist = new ArrayList<>(v2rackMsg.getChecklist());
            // REMOVE THE FIRST ELEMENT WHICH IS THE CURRENT NEXT HOP
            updatedChecklist.remove(0);
            // SET THE NEW NEXT HOP: IF THE UPDATED CHECKLIST IS EMPTY, THEN RSU DESTINATION BECOMES THE NEXT HOP
            String newNextHop = updatedChecklist.isEmpty() ? v2rackMsg.getRsuDestination() : updatedChecklist.get(0);
            MessageRouting newRouting = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
            V2RACK newV2rackMsg = new V2RACK(
                newRouting,
                v2rackMsg.getUniqueId(),
                v2rackMsg.getOriginalMsgId(),
                v2rackMsg.getTimeStamp(),
                v2rackMsg.getTimestampLimit(),
                v2rackMsg.getVehicleId(),
                v2rackMsg.getRsuDestination(),
                newNextHop,
                updatedChecklist
            );
            getOs().getAdHocModule().sendV2xMessage(newV2rackMsg);
            logInfo("V2RACK MESSAGE FORWARDED TO NEXT HOP " + newNextHop +
                    " WITH UPDATED CHECKLIST: " + updatedChecklist.toString());
            return;        
        }       
    }

    /**
     * HANDLES THE UPSTREAM ACKNOWLEDGEMENT PROCESS.
     * THIS FUNCTION IS CALLED WHEN AN R2V MESSAGE REACHES ITS FINAL DESTINATION.
     * IT INVERTS THE FORWARDING TRAIL (CHECKLIST) FROM THE R2V MESSAGE TO OBTAIN THE UPSTREAM PATH,
     * CREATES A V2RACK MESSAGE, AND SENDS IT UPSTREAM TOWARD THE RSU.
     */
    private void handleUpstreamAck(R2VMsg r2vMsg) {
        String currentVehId = getOs().getId().toUpperCase();
        long currentTime = getOs().getSimulationTime();
        // INVERT THE FORWARDING TRAIL FROM THE RECEIVED R2V MESSAGE TO OBTAIN THE UPSTREAM PATH
        List<String> originalTrail = r2vMsg.getForwardingTrail();
        List<String> reversedTrail = new ArrayList<>(originalTrail);
        Collections.reverse(reversedTrail);
        // SET THE NEXT HOP FOR THE ACKNOWLEDGEMENT AS THE FIRST ELEMENT IN THE REVERSED TRAIL, IF AVAILABLE
        String nextHop;
        if (!reversedTrail.isEmpty()) {
            nextHop = reversedTrail.get(0);
        } else {
            // IF THE REVERSED TRAIL IS EMPTY, ASSUME THAT THE RSU IS THE DESTINATION
            nextHop = "RSU_0";
        }
        // DEFINE THE RSU DESTINATION; ASSUMING STATIC RSU INFORMATION (E.G., "RSU_0")
        String rsuDestination = "RSU_0";
        // CREATE A UNIQUE ID FOR THE V2RACK MESSAGE
        String v2rackUniqueId = "V2RACK-" + v2rCounter.incrementAndGet();
        // DEFINE TIMESTAMP LIMIT, FOR INSTANCE, CURRENT TIME PLUS 10 SECONDS (10,000 MS)
        long timestampLimit = currentTime + 10000L;
        // CREATE THE V2RACK MESSAGE USING THE NEW V2RACK CLASS
        V2RACK v2rackMsg = new V2RACK(
            getOs().getAdHocModule().createMessageRouting().viaChannel(AdHocChannel.CCH).topoBroadCast(),
            v2rackUniqueId,
            r2vMsg.getUniqueId(), // ORIGINAL MESSAGE ID FROM THE R2V MESSAGE
            currentTime,
            timestampLimit,
            currentVehId, // VEHICLE ID OF THE ACK SENDER
            rsuDestination,
            nextHop,
            reversedTrail // THE INVERTED FORWARDING TRAIL (CHECKLIST)
        );
        // SEND THE V2RACK MESSAGE UPSTREAM
        getOs().getAdHocModule().sendV2xMessage(v2rackMsg);
        logInfo("V2RACK MESSAGE SENT UPSTREAM: " + v2rackMsg.toString());
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        logDebug("MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        logDebug("ACKNOWLEDGEMENT RECEIVED: " + ack.toString());
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        logDebug("CAM EVENT TRIGGERED");
    }
}