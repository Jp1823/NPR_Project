package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.VehApp.VehicleGraph.NodeRecord;
import pt.uminho.npr.tutorial.Messages.R2VMsg;
import pt.uminho.npr.tutorial.Messages.V2RACK;
import pt.uminho.npr.tutorial.Messages.V2VExt;
import pt.uminho.npr.tutorial.Messages.DepthNeighMsg;
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
 * VEHICLE APPLICATION IMPLEMENTATION FOR V2X COMMUNICATION, NEIGHBOR MANAGEMENT, AND INTELLIGENT MESSAGE FORWARDING.
 * THIS VERSION INTEGRATES A NEW FLOODING MECHANISM USING DEPTHNEIGHMSG MESSAGES.
 * EACH VEHICLE PERIODICALLY (EVERY SECOND) CREATES A NEW DEPTHNEIGHMSG WITH TTL SET TO 3, USING A UNIQUE MESSAGE ID,
 * AND FLOODS THE NETWORK WITH ITS CURRENT VEHICLEGRAPH DATA. A LOCAL CACHE OF PROCESSED MESSAGE IDS IS MAINTAINED
 * TO PREVENT CYCLES AND DUPLICATE REBROADCASTS.
 */
public class VehApp extends AbstractApplication<VehicleOperatingSystem>
        implements VehicleApplication, CommunicationApplication {

    // CONSTANTS
    private final long MSG_DELAY = 100 * TIME.MILLI_SECOND;
    private final int TX_POWER = 100;
    private final double TX_RANGE = 150.0;
    private final double RSU_RANGE = 100.0;
    private final long CLEAN_THRESHOLD = 1000 * TIME.MILLI_SECOND;
    // DEPTH MESSAGE TTL (SET TO 3)
    private final int DEPTH_MSG_TTL = 8;
    // PERIOD FOR SENDING DEPTHNEIGH MESSAGES (EVERY 1 SECOND)
    private final long DEPTH_MSG_PERIOD = 1000 * TIME.MILLI_SECOND;

    // VEHICLE STATE VARIABLES
    private boolean startSending = false;
    private double vehHeading;
    private double vehSpeed;
    private int vehLane;

    // STATIC RSU INFORMATION (FOR INSTANCE, A FIXED RSU)
    private final Map<String, GeoPoint> staticRsus = new HashMap<>();

    // VEHICLEGRAPH: MANAGES ALL NEIGHBOR RECORDS AND AGGREGATED INFORMATION
    private final VehicleGraph vehicleGraph = new VehicleGraph();

    // FRIENDSFRIENDS: MANAGES ALL NEIGHBORS INFORMATIONS
    private final VehicleGraph friendsFriends = new VehicleGraph();

    // LOCAL NEIGHBOR SET: BUILT FROM DIRECT V2VEXT MESSAGES
    private final Set<String> localNeighborSet = new HashSet<>();

    // PROCESSED DEPTHNEIGH MESSAGE IDS TO AVOID CYCLES
    private final Set<String> processedDepthMsgIDs = new HashSet<>();

    // COUNTERS
    private static final AtomicInteger v2rCounter = new AtomicInteger(0);

    // TIMERS
    private long lastDepthMsgTime = 0;

    /**
     * VEHICLEGRAPH CLASS.
     * THIS CLASS CENTRALIZES THE MANAGEMENT OF NEIGHBOR INFORMATION, INCLUDING:
     * - DISTANCE FROM THE CURRENT VEHICLE TO THE NEIGHBOR,
     * - NEIGHBOR'S DISTANCE TO THE NEAREST RSU,
     * - RSU REACHABILITY,
     * - THE AGGREGATED NEIGHBOR LIST,
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
         * ADJACENCY IS BASED ON THE AGGREGATED NEIGHBOR LIST OF EACH NODE.
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
     * LOGS A MESSAGE AT THE INFO LEVEL.
     *
     * @param message THE MESSAGE TO LOG.
     */
    private void logInfo(String message) {
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [INFO] " + message.toUpperCase());
    }

    /**
     * LOGS A MESSAGE AT THE DEBUG LEVEL.
     *
     * @param message THE MESSAGE TO LOG.
     */
    private void logDebug(String message) {
        String vehId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + vehId + "] [DEBUG] " + message.toUpperCase());
    }

    /**
     * ADDS STATIC RSUS TO THE VEHICLE GRAPH.
     * CALCULATES THE DISTANCE FROM THE CURRENT VEHICLE POSITION TO EACH STATIC RSU,
     * DETERMINES REACHABILITY, AND UPDATES THE VEHICLE GRAPH.
     */
    private void addStaticRsusToGraph() {
        GeoPoint currentPos = getOs().getPosition();
        long currentTime = getOs().getSimulationTime();
        for (Map.Entry<String, GeoPoint> entry : staticRsus.entrySet()) {
            String rsuId = entry.getKey().toUpperCase();
            GeoPoint rsuPos = entry.getValue();
            double distance = computeDistance(currentPos, rsuPos);
            boolean reachable = distance <= RSU_RANGE;
            VehicleGraph.NodeRecord rsuRecord = new VehicleGraph.NodeRecord(
                    distance, distance, reachable, new ArrayList<>(), currentTime
            );
            vehicleGraph.addOrUpdateNode(rsuId, rsuRecord);
            // logDebug("ADDED STATIC RSU TO GRAPH: " + rsuId + " WITH DISTANCE: " + String.format("%.2f", distance));
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
        staticRsus.put("RSU_0", GeoPoint.latLon(52.454223, 13.313477, 0));
        addStaticRsusToGraph();
        logInfo("VEHICLE APP STARTUP");
        lastDepthMsgTime = getOs().getSimulationTime();
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        printNeighborGraph();
        printSuperNeighborList();
        printProcessedDepthMsgIds();
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
            addStaticRsusToGraph();
            vehicleGraph.removeStaleNodes(currentTime, CLEAN_THRESHOLD);
            sendV2VExtMsg();
            // SEND DEPTHNEIGH MESSAGE EVERY DEPTH_MSG_PERIOD (1 SECOND)
            if (currentTime - lastDepthMsgTime >= DEPTH_MSG_PERIOD) {
                sendDepthNeighMsg();
                lastDepthMsgTime = currentTime;
            }
        }
        // printNeighborGraph();
        // printSuperNeighborList();
        printFriendsFriendsGraph();
        // printProcessedDepthMsgIds();
        getOs().getEventManager().addEvent(currentTime + MSG_DELAY, this);
    }

    /**
     * SENDS A V2V EXTENDED MESSAGE CONTAINING LOCAL NEIGHBOR INFORMATION.
     */
    private void sendV2VExtMsg() {
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
            .viaChannel(AdHocChannel.CCH)
            .topoBroadCast();
        long currentTime = getOs().getSimulationTime();
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
        // logDebug("SENT V2VEXT: " + extMsg.toString() + " AT TIME " + currentTime);
    }

    /**
     * MODIFIED METHOD TO SEND A DEPTHNEIGH MESSAGE.
     * THE MESSAGE ID IS NOW GENERATED AS "DN_" + SENDER VEHICLE ID + "_" + DEPTH MESSAGE COUNTER,
     * WITHOUT INCLUDING THE TIMESTAMP IN THE ID.
     */
    
    private final AtomicInteger depthMsgCounter = new AtomicInteger(0);

    private void sendDepthNeighMsg() {
        MessageRouting routing = getOs().getAdHocModule()
            .createMessageRouting()
            .viaChannel(AdHocChannel.CCH)
            .topoBroadCast();
    
        long currentTime = getOs().getSimulationTime();
        String depthMsgId = "DN_" + getOs().getId().toUpperCase() + "_" + depthMsgCounter.incrementAndGet();
    
        // BUILD NODE RECORDS MAP
        Map<String, NodeRecord> graphData = new HashMap<>();
        for (var entry : vehicleGraph.getNodesSnapshot().entrySet()) {
            String vid = entry.getKey();
            var vr  = entry.getValue();
            graphData.put(vid, new NodeRecord(
                vr.getDistanceToVehicle(),
                vr.getRsuDistance(),
                vr.isRsuReachable(),
                vr.getNeighborList(),
                vr.getTimestamp()
            ));
        }
    
        DepthNeighMsg depthMsg = new DepthNeighMsg(
            routing,
            depthMsgId,
            currentTime,
            getOs().getId().toUpperCase(),
            DEPTH_MSG_TTL,
            graphData
        );
    
        processedDepthMsgIDs.add(depthMsgId);
        getOs().getAdHocModule().sendV2xMessage(depthMsg);
        logInfo("SENT DEPTHNEIGHMSG: " + depthMsg.toString());
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
     * DETERMINES THE NEAREST RSU FOR A GIVEN GEOPOINT.
     *
     * @param pos          TARGET GEOPOINT.
     * @param msgTimestamp TIMESTAMP OF THE MESSAGE.
     * @return A NODE RECORD CONTAINING NEAREST RSU INFORMATION.
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
        // logDebug("COMPUTED NEAREST RSU: " + bestRsu + " WITH DISTANCE: " + String.format("%.2f", minDistance) + "M, REACHABLE: " + (reachable ? "YES" : "NO") + ", MSG TIMESTAMP: " + msgTimestamp);
        return new VehicleGraph.NodeRecord(0, minDistance, reachable, new ArrayList<>(), msgTimestamp);
    }

    /**
     * PRINTS THE CURRENT VEHICLEGRAPH (ALL NODE RECORDS).
     */
    private void printNeighborGraph() {
        StringBuilder sb = new StringBuilder();
        Map<String, VehicleGraph.NodeRecord> snapshot = vehicleGraph.getNodesSnapshot();
        for (Map.Entry<String, VehicleGraph.NodeRecord> entry : snapshot.entrySet()) {
            sb.append(entry.getKey().toUpperCase())
              .append(" -> ")
              .append(entry.getValue().toString())
              .append("\n");
        }
        logInfo("[VEHICLEGRAPH] \n" + sb.toString());
    }

    /**
     * PRINTS THE PROCESSED DEPTH MESSAGE IDS IN ASCENDING ORDER.
     * THIS METHOD SORTS THE IDS FIRST BY SENDER (EXTRACTED FROM THE ID) AND THEN BY THE NUMERICAL MESSAGE COUNTER.
     * THE EXPECTED MESSAGE ID FORMAT IS "DN_{SENDER}_{COUNTER}".
     */
    private void printProcessedDepthMsgIds() {
        StringBuilder sb = new StringBuilder();
        sb.append("[PROCESSED DEPTH MSG IDS]\n");
        
        // CREATE A LIST FROM THE PROCESSED MESSAGE IDS SET
        List<String> sortedIds = new ArrayList<>(processedDepthMsgIDs);
        
        // SORT THE MESSAGE IDS USING A CUSTOM COMPARATOR
        Collections.sort(sortedIds, (id1, id2) -> {
            // SPLIT THE IDS BY THE UNDERSCORE DELIMITER
            // EXPECTED FORMAT: "DN_{SENDER}_{COUNTER}"
            String[] parts1 = id1.split("_");
            String[] parts2 = id2.split("_");
            
            // ENSURE THE FORMAT HAS AT LEAST 3 PARTS: ["DN", SENDER_PART, COUNTER]
            if (parts1.length < 3 || parts2.length < 3) {
                return id1.compareToIgnoreCase(id2);
            }
            
            // EXTRACT THE SENDER IDENTIFIER
            // IF THE SENDER CONTAINS UNDERSCORES, WE CONSIDER ALL PARTS BETWEEN THE FIRST AND THE LAST
            String sender1 = String.join("_", Arrays.copyOfRange(parts1, 1, parts1.length - 1));
            String sender2 = String.join("_", Arrays.copyOfRange(parts2, 1, parts2.length - 1));
            
            // COMPARE THE SENDER IDENTIFIERS
            int senderComparison = sender1.compareToIgnoreCase(sender2);
            if (senderComparison != 0) {
                return senderComparison;
            }
            
            // EXTRACT THE NUMERICAL COUNTER (ASSUMING THE LAST PART IS THE COUNTER)
            try {
                int counter1 = Integer.parseInt(parts1[parts1.length - 1]);
                int counter2 = Integer.parseInt(parts2[parts2.length - 1]);
                return Integer.compare(counter1, counter2);
            } catch (NumberFormatException e) {
                return id1.compareToIgnoreCase(id2);
            }
        });
        
        // APPEND THE SORTED IDS TO THE STRING BUILDER
        for (String id : sortedIds) {
            sb.append(id).append("\n");
        }
        
        // LOG THE SORTED PROCESSED MESSAGE IDS
        logInfo(sb.toString());
    }

    /**
     * PRINTS THE SUPER NEIGHBOR LIST, WHICH IS THE UNION OF THE LOCAL NEIGHBOR SET AND ALL NEIGHBOR LISTS FROM THE VEHICLEGRAPH.
     */
    private void printSuperNeighborList() {
        Set<String> superSet = new HashSet<>();
        superSet.addAll(localNeighborSet);
        for (VehicleGraph.NodeRecord node : vehicleGraph.getNodesSnapshot().values()) {
            superSet.addAll(node.getNeighborList());
        }
        List<String> superList = new ArrayList<>(superSet);
        Collections.sort(superList);
        logInfo("[SUPER NEIGHBOR LIST] " + superList.toString());
    }

    /**
     * PRINTS THE CURRENT FRIENDSFRIENDS GRAPH, WHICH CONTAINS THE AGGREGATED DEPTH NEIGHBOR INFORMATION
     * RECEIVED FROM GLOBAL DEPTHNEIGHMSG MESSAGES. THIS METHOD ITERATES OVER ALL ENTRIES IN THE FRIENDSFRIENDS
     * STRUCTURE AND LOGS THEIR DETAILS.
     */
    private void printFriendsFriendsGraph() {
        StringBuilder sb = new StringBuilder();
        sb.append("[FRIENDSFRIENDS GRAPH]\n");
        Map<String, VehicleGraph.NodeRecord> snapshot = friendsFriends.getNodesSnapshot();
        
        // SPLIT THE KEYS INTO VEHICLE KEYS AND RSU KEYS
        List<String> vehicleKeys = new ArrayList<>();
        List<String> rsuKeys = new ArrayList<>();
        
        for (String key : snapshot.keySet()) {
            if (key.toUpperCase().startsWith("VEH_")) {
                vehicleKeys.add(key);
            } else if (key.toUpperCase().startsWith("RSU_")) {
                rsuKeys.add(key);
            } else {
                // IF THE KEY DOES NOT MATCH THE PATTERN, TREAT IT AS VEHICLE BY DEFAULT
                vehicleKeys.add(key);
            }
        }
        
        // SORT VEHICLE KEYS IN ASCENDING NUMERICAL ORDER BASED ON THE NUMBER AFTER "VEH_"
        Collections.sort(vehicleKeys, (a, b) -> {
            try {
                int numA = Integer.parseInt(a.toUpperCase().replace("VEH_", ""));
                int numB = Integer.parseInt(b.toUpperCase().replace("VEH_", ""));
                return Integer.compare(numA, numB);
            } catch (NumberFormatException e) {
                // FALLBACK: COMPARE STRINGS LEXICOGRAPHICALLY
                return a.compareToIgnoreCase(b);
            }
        });
        
        // SORT RSU KEYS ALPHABETICALLY
        Collections.sort(rsuKeys, String::compareToIgnoreCase);
        
        // PRINT VEHICLE KEYS FIRST
        for (String key : vehicleKeys) {
            sb.append(key.toUpperCase())
              .append(" -> ")
              .append(snapshot.get(key).toString())
              .append("\n");
        }
        
        // PRINT RSU KEYS LAST
        for (String key : rsuKeys) {
            sb.append(key.toUpperCase())
              .append(" -> ")
              .append(snapshot.get(key).toString())
              .append("\n");
        }
        
        logInfo(sb.toString());
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rcvMsg) {
        V2xMessage msg = rcvMsg.getMessage();
        if (msg instanceof V2VExt) {
            V2VExt extMsg = (V2VExt) msg;
            String senderId = extMsg.getSenderName().toUpperCase();
            long msgTimestamp = extMsg.getTimeStamp();
            double distanceToSender = computeDistance(getOs().getPosition(), extMsg.getSenderPos());
            VehicleGraph.NodeRecord rsuInfo = computeNearestRsu(extMsg.getSenderPos(), msgTimestamp);
            
            List<String> newNeighborList = new ArrayList<>();
            if (extMsg.getNeighborList() != null) {
                for (String n : extMsg.getNeighborList()) {
                    newNeighborList.add(n.toUpperCase());
                }
            }
            // UPDATE LOCAL NEIGHBOR SET WITH THE SENDER ID
            localNeighborSet.add(senderId);
            
            List<String> mergedNeighborList = new ArrayList<>(newNeighborList);
            VehicleGraph.NodeRecord existingRecord = vehicleGraph.getNodesSnapshot().get(senderId);
            if (existingRecord != null) {
                Set<String> unionSet = new HashSet<>(existingRecord.getNeighborList());
                unionSet.addAll(newNeighborList);
                mergedNeighborList = new ArrayList<>(unionSet);
            }
            VehicleGraph.NodeRecord nodeRecord = new VehicleGraph.NodeRecord(
                distanceToSender,
                rsuInfo.getRsuDistance(),
                rsuInfo.isRsuReachable(),
                mergedNeighborList,
                msgTimestamp
            );
            vehicleGraph.addOrUpdateNode(senderId, nodeRecord);
            // logDebug("UPDATED VEHICLEGRAPH NODE FOR " + senderId + " WITH RECORD: " + nodeRecord.toString());
        } else if (msg instanceof DepthNeighMsg) {
            DepthNeighMsg dMsg = (DepthNeighMsg) msg;
            String msgId = dMsg.getMessageId();
        
            // AVOID PROCESSING DUPLICATES
            if (processedDepthMsgIDs.contains(msgId)) {
                logDebug("DEPTHNEIGHMSG " + msgId + " ALREADY PROCESSED; IGNORING");
                return;
            }
            processedDepthMsgIDs.add(msgId);
        
            // TTL CHECK
            if (dMsg.getTtl() <= 0) {
                logDebug("DEPTHNEIGHMSG " + msgId + " TTL <= 0; STOPPING FLOOD");
                return;
            }
        
            // EXTRACT NODE RECORDS MAP
            Map<String, VehicleGraph.NodeRecord> receivedMap = dMsg.getDepthNeigh();
        
            // MERGE INTO friendsFriends GRAPH
            for (Map.Entry<String, VehicleGraph.NodeRecord> entry : receivedMap.entrySet()) {
                String nodeId = entry.getKey();
                VehicleGraph.NodeRecord incoming = entry.getValue();
                VehicleGraph.NodeRecord existing = friendsFriends.getNodesSnapshot().get(nodeId);
        
                if (existing != null) {
                    // MERGE NEIGHBOR LISTS (UNION, UPPERCASE)
                    Set<String> union = new HashSet<>(existing.getNeighborList());
                    union.addAll(incoming.getNeighborList());
                    List<String> mergedNeighbors = new ArrayList<>(union);
        
                    // CHOOSE MOST RECENT TIMESTAMP
                    long latestTimestamp = Math.max(existing.getTimestamp(), incoming.getTimestamp());
        
                    // OPTIONALLY YOU COULD PICK MINIMUM DISTANCES OR ACTUAL INCOMING VALUES:
                    double distToVeh    = incoming.getDistanceToVehicle();
                    double rsuDist      = incoming.getRsuDistance();
                    boolean reachable   = incoming.isRsuReachable();
        
                    VehicleGraph.NodeRecord updated =
                        new VehicleGraph.NodeRecord(distToVeh, rsuDist, reachable, mergedNeighbors, latestTimestamp);
                    friendsFriends.addOrUpdateNode(nodeId, updated);
        
                } else {
                    // FIRST TIME SEEING THIS NODE
                    VehicleGraph.NodeRecord fresh =
                        new VehicleGraph.NodeRecord(
                            incoming.getDistanceToVehicle(),
                            incoming.getRsuDistance(),
                            incoming.isRsuReachable(),
                            incoming.getNeighborList(),
                            incoming.getTimestamp()
                        );
                    friendsFriends.addOrUpdateNode(nodeId, fresh);
                }
            }
        
            logDebug("MERGED DEPTHNEIGHMSG " + msgId + " INTO FRIENDSFRIENDS");
        
            // FORWARD WITH TTL-1
            int newTtl = dMsg.getTtl() - 1;
            if (newTtl > 0) {
                MessageRouting newRouting = getOs().getAdHocModule().createMessageRouting()
                    .viaChannel(AdHocChannel.CCH)
                    .topoBroadCast();
                DepthNeighMsg forward = new DepthNeighMsg(
                    newRouting,
                    msgId,
                    dMsg.getTimeStamp(),
                    dMsg.getSenderId(),
                    newTtl,
                    receivedMap
                );
                getOs().getAdHocModule().sendV2xMessage(forward);
                logInfo("FORWARDED DEPTHNEIGHMSG " + msgId + " WITH TTL=" + newTtl);
            } else {
                logDebug("DEPTHNEIGHMSG " + msgId + " TTL EXHAUSTED; NOT FORWARDING");
            }
        } else if (msg instanceof R2VMsg) {
            R2VMsg r2vMsg = (R2VMsg) msg;
            String currentVehId = getOs().getId().toUpperCase();
            if (!r2vMsg.getNextHop().equalsIgnoreCase(currentVehId)) {
                // logDebug("CURRENT VEHICLE (" + currentVehId + ") IS NOT THE NEXT HOP (" + r2vMsg.getNextHop().toUpperCase() + "); DROPPING MESSAGE");
                return;
            }
            if (r2vMsg.getVehDestination().equalsIgnoreCase(currentVehId)) {
                // logInfo("FINAL DELIVERY R2V MESSAGE RECEIVED: " + r2vMsg.toString());
                handleUpstreamAck(r2vMsg);
                return;
            }
            if (r2vMsg.getForwardingTrail().contains(currentVehId)) {
                // logDebug("MESSAGE ALREADY PASSED THROUGH " + currentVehId + "; DROPPING MESSAGE");
                return;
            }
            String vehDestination = r2vMsg.getVehDestination();
            if (vehicleGraph.getNodesSnapshot().containsKey(vehDestination.toUpperCase())) {
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
                        r2vMsg.getRsuSource(),
                        vehDestination,
                        vehDestination,
                        r2vMsg.getOrder(),
                        updatedTrail
                );
                getOs().getAdHocModule().sendV2xMessage(directR2vMsg);
                // logInfo("DIRECT R2V MESSAGE FORWARDED TO VEHICLE DESTINATION " + vehDestination + " WITH UPDATED TRAIL: " + updatedTrail.toString());
                return;
            }
            int bestHops = Integer.MAX_VALUE;
            double bestDistance = Double.MAX_VALUE;
            String bestCandidate = null;
            Map<String, NodeRecord> nodesSnapshot = vehicleGraph.getNodesSnapshot();
            for (Map.Entry<String, NodeRecord> entry : nodesSnapshot.entrySet()) {
                String candidate = entry.getKey();
                int hops = vehicleGraph.getMinHopCount(candidate, vehDestination);
                if (hops == Integer.MAX_VALUE) continue;
                double candidateDistance = entry.getValue().getDistanceToVehicle();
                if (hops < bestHops || (hops == bestHops && candidateDistance < bestDistance)) {
                    bestHops = hops;
                    bestDistance = candidateDistance;
                    bestCandidate = candidate;
                }
            }
            if (bestCandidate == null) {
                // logInfo("NO CANDIDATE FOUND TO FORWARD R2V MESSAGE TO VEHICLE DESTINATION " + vehDestination);
                return;
            }
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
                    r2vMsg.getRsuSource(),
                    vehDestination,
                    bestCandidate,
                    r2vMsg.getOrder(),
                    updatedTrail
            );
            getOs().getAdHocModule().sendV2xMessage(newR2vMsg);
            // logInfo("R2V MESSAGE FORWARDED TO " + bestCandidate + " WITH UPDATED TRAIL: " + updatedTrail.toString());
            return;
        } else if (msg instanceof V2RACK) {
            V2RACK v2rackMsg = (V2RACK) msg;
            String currentVehId = getOs().getId().toUpperCase();
            if (!v2rackMsg.getNextHop().equalsIgnoreCase(currentVehId)) {
                // logDebug("CURRENT VEHICLE (" + currentVehId + ") IS NOT THE NEXT HOP (" + v2rackMsg.getNextHop().toUpperCase() + "); DROPPING V2RACK MESSAGE");
                return;
            }
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
                        v2rackMsg.getRsuDestination(),
                        v2rackMsg.getChecklist()
                );
                getOs().getAdHocModule().sendV2xMessage(directV2rackMsg);
                // logInfo("DIRECT V2RACK MESSAGE FORWARDED TO RSU DESTINATION " + v2rackMsg.getRsuDestination() + " WITH EMPTY CHECKLIST");
                return;
            }
            List<String> updatedChecklist = new ArrayList<>(v2rackMsg.getChecklist());
            updatedChecklist.remove(0);
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
            // logInfo("V2RACK MESSAGE FORWARDED TO NEXT HOP " + newNextHop + " WITH UPDATED CHECKLIST: " + updatedChecklist.toString());
            return;
        }
    }

    /**
     * HANDLES THE UPSTREAM ACKNOWLEDGEMENT PROCESS.
     * THIS FUNCTION IS CALLED WHEN AN R2V MESSAGE REACHES ITS FINAL DESTINATION.
     * IT INVERTS THE FORWARDING TRAIL TO OBTAIN THE UPSTREAM PATH, CREATES A V2RACK MESSAGE,
     * AND SENDS IT UPSTREAM TOWARD THE RSU.
     */
    private void handleUpstreamAck(R2VMsg r2vMsg) {
        String currentVehId = getOs().getId().toUpperCase();
        long currentTime = getOs().getSimulationTime();
        List<String> originalTrail = r2vMsg.getForwardingTrail();
        List<String> reversedTrail = new ArrayList<>(originalTrail);
        Collections.reverse(reversedTrail);
        String nextHop = reversedTrail.isEmpty() ? "RSU_0" : reversedTrail.get(0);
        String rsuDestination = "RSU_0";
        String v2rackUniqueId = "V2RACK-" + v2rCounter.incrementAndGet();
        long timestampLimit = currentTime + 10000L;
        MessageRouting newRouting = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        V2RACK v2rackMsg = new V2RACK(
                newRouting,
                v2rackUniqueId,
                r2vMsg.getUniqueId(),
                currentTime,
                timestampLimit,
                currentVehId,
                rsuDestination,
                nextHop,
                reversedTrail
        );
        getOs().getAdHocModule().sendV2xMessage(v2rackMsg);
        // logInfo("V2RACK MESSAGE SENT UPSTREAM: " + v2rackMsg.toString());
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {}

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {}

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {}
}