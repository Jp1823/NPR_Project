package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.Messages.F2RMsg;
import pt.uminho.npr.tutorial.Messages.R2VMsg;
import pt.uminho.npr.tutorial.Messages.V2RACK;
import pt.uminho.npr.tutorial.Messages.V2VExt;
import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RSUAPP IMPLEMENTATION FOR V2X COMMUNICATION.
 * THIS APPLICATION RUNS ON A ROAD-SIDE UNIT (RSU) RESPONSIBLE FOR:
 * - RECEIVING F2R MESSAGES FROM FOG NODES,
 * - UPDATING AND MANAGING NEIGHBOR INFORMATION BASED ON V2VEXT MESSAGES,
 * - FORWARDING R2V MESSAGES TO VEHICLES,
 * - AND PROCESSING UPSTREAM ACKNOWLEDGEMENTS (V2RACK) FROM VEHICLES.
 *
 * NEIGHBOR RECORDS ARE MANAGED VIA THE INNER EXTENDEDNEIGHBORRECORD CLASS,
 * WHICH STORES THE VEHICLE ID, DISTANCE, LAST UPDATE TIMESTAMP, AND A SIMPLE LIST OF NEIGHBORS.
 *
 * ALL LOGGING IS STANDARDIZED WITH INFO (CRITICAL EVENTS) AND DEBUG (DETAILED PROCESSING),
 * WITH MESSAGES WRITTEN IN PERFECT, ALL‑CAPS ENGLISH.
 */
public class RsuApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    // CONSTANTS
    private final long MSG_DELAY = 500 * TIME.MILLI_SECOND;
    private final int TX_POWER = 70;
    private final double TX_RANGE = 200.0;
    private final long STALE_THRESHOLD = 1000 * TIME.MILLI_SECOND;

    // ACTIVE NEIGHBOR RECORDS (KEY: VEHICLE ID)
    private final Map<String, ExtendedNeighborRecord> neighborRecords = new HashMap<>();

    // ARCHIVE FOR SENT R2V MESSAGES (KEY: UNIQUE R2V MESSAGE ID)
    private final Map<String, R2VMsg> sentR2VArchive = new HashMap<>();

    // ATOMIC COUNTER FOR UNIQUE R2V MESSAGE IDS
    private static final AtomicInteger r2vCounter = new AtomicInteger(0);

    /**
     * EXTENDEDNEIGHBORRECORD CLASS.
     * STORES NEIGHBOR INFORMATION:
     * - VEHICLE ID,
     * - DISTANCE TO THE NEIGHBOR,
     * - LAST UPDATE TIMESTAMP,
     * - NEIGHBOR LIST (LIST OF NEIGHBOR VEHICLE IDS).
     */
    public static class ExtendedNeighborRecord {
        private final String id;
        private double distance;
        private long lastUpdateTimestamp;
        private List<String> neighborList;

        public ExtendedNeighborRecord(String id, double distance, long lastUpdateTimestamp, List<String> neighborList) {
            this.id = id;
            this.distance = distance;
            this.lastUpdateTimestamp = lastUpdateTimestamp;
            // STORE ALL NEIGHBOR IDS IN UPPERCASE
            this.neighborList = new ArrayList<>();
            for (String neighbor : neighborList) {
                this.neighborList.add(neighbor.toUpperCase());
            }
        }

        public String getId() {
            return id;
        }

        public double getDistance() {
            return distance;
        }

        public void setDistance(double distance) {
            this.distance = distance;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public List<String> getNeighborList() {
            return neighborList;
        }

        public void setNeighborList(List<String> neighborList) {
            // UPDATE LIST ENSURING ALL ENTRIES ARE IN UPPERCASE AND UNIQUE
            Set<String> unique = new HashSet<>();
            for (String neighbor : neighborList) {
                unique.add(neighbor.toUpperCase());
            }
            this.neighborList = new ArrayList<>(unique);
        }

        @Override
        public String toString() {
            return String.format("[ID: %s] [DISTANCE: %.2fM] [LAST UPDATE: %d] [NEIGHBOR LIST: %s]",
                    id.toUpperCase(), distance, lastUpdateTimestamp, neighborList.toString().toUpperCase());
        }
    }

    // ==================== LOGGING METHODS ====================

    /**
     * LOGS A MESSAGE AT THE INFO LEVEL (CRITICAL EVENTS, INTERNAL STRUCTURE PRINTS, ETC.).
     *
     * @param message THE MESSAGE TO LOG.
     */
    private void logInfo(String message) {
        String rsuId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + rsuId + "] [INFO] " + message.toUpperCase());
    }

    /**
     * LOGS A MESSAGE AT THE DEBUG LEVEL (DETAILED PROCESSING INFORMATION).
     *
     * @param message THE MESSAGE TO LOG.
     */
    private void logDebug(String message) {
        String rsuId = getOs().getId().toUpperCase();
        getLog().infoSimTime(this, "[" + rsuId + "] [DEBUG] " + message.toUpperCase());
    }

    // ==================== RSUAPP LIFECYCLE METHODS ====================

    @Override
    public void onStartup() {
        // ENABLE THE AD-HOC RADIO MODULE
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER)
                .distance(TX_RANGE)
                .create()
        );
        logInfo("RSU APP STARTUP");
        // SCHEDULE FIRST EVENT
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        printNeighborRecords();
        logInfo("RSU APP SHUTDOWN");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) throws Exception {
        removeStaleNeighbors();
        printNeighborRecords();
        // SCHEDULE NEXT EVENT
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    // ==================== MESSAGE HANDLING METHODS ====================

    @Override
    public void onMessageReceived(ReceivedV2xMessage rcvMsg) {
        V2xMessage msg = rcvMsg.getMessage();
        if (msg instanceof F2RMsg) {
            handleF2RMessage((F2RMsg) msg);
        } else if (msg instanceof V2VExt) {
            handleV2VExtMessage((V2VExt) msg);
        } else if (msg instanceof V2RACK) {
            V2RACK ackMsg = (V2RACK) msg;
            String rsuId = getOs().getId().toUpperCase();
            // IF THE V2RACK MESSAGE IS NOT INTENDED FOR THIS RSU, DROP IT
            if (!ackMsg.getNextHop().equalsIgnoreCase(rsuId)) {
                logDebug("V2RACK MESSAGE NOT INTENDED FOR THIS RSU (" + rsuId + ") AS NEXT HOP; DROPPING MESSAGE");
                return;
            }
            // VERIFY THAT THE ORIGINAL MESSAGE ID WAS ACTUALLY EMITTED
            if (!sentR2VArchive.containsKey(ackMsg.getOriginalMsgId())) {
                logInfo("RECEIVED V2RACK MESSAGE WITH ORIGINAL MESSAGE ID " + ackMsg.getOriginalMsgId() +
                        " WHICH WAS NOT EMITTED; POSSIBLE ERROR");
                return;
            }
            // RETRIEVE THE ORIGINAL R2V MESSAGE FROM THE ARCHIVE
            R2VMsg originalMsg = sentR2VArchive.get(ackMsg.getOriginalMsgId());
            // VERIFY THAT THE ORIGINAL DESTINATION MATCHES THE VEHICLE ID FROM THE ACK MESSAGE
            if (!originalMsg.getVehDestination().equalsIgnoreCase(ackMsg.getVehicleId())) {
                logInfo("V2RACK MESSAGE ORIGINAL DESTINATION MISMATCH: EXPECTED " + originalMsg.getVehDestination() +
                        " BUT GOT " + ackMsg.getVehicleId());
                return;
            }
            // IF ALL CHECKS PASS, PRINT A DETAILED LOG MESSAGE OF ACK RECEIPT WITH MAXIMUM CONTENT
            logInfo("V2RACK MESSAGE RECEIVED AT RSU: " + ackMsg.toString() +
                    " | ORIGINAL R2V MESSAGE: " + originalMsg.toString());
        }
    }

    /**
     * HANDLES F2R MESSAGES RECEIVED FROM FOG NODES.
     *
     * @param f2rMsg THE RECEIVED F2R MESSAGE.
     */
    private void handleF2RMessage(F2RMsg f2rMsg) {
        String rsuId = getOs().getId().toUpperCase();
        // VERIFY THAT THE MESSAGE IS INTENDED FOR THIS RSU
        if (!f2rMsg.getRsuDestination().equalsIgnoreCase(rsuId)) {
            return;
        }
        logInfo("F2R MESSAGE RECEIVED: " + f2rMsg.toString());

        // BEGIN FORWARDING PROCESS FOR R2V MESSAGE
        String vehDest = f2rMsg.getVehicleDestination();
        logDebug("VEHICLE DESTINATION: " + vehDest);
        String nextHop;
        if (neighborRecords.containsKey(vehDest)) {
            nextHop = vehDest;
            logInfo("VEHICLE DESTINATION DIRECTLY REACHABLE: " + vehDest);
        } else {
            logInfo("VEHICLE DESTINATION NOT DIRECTLY REACHABLE. SEARCHING FOR NEXT HOP FOR: " + vehDest);
            nextHop = decideNextHop(vehDest);
            if (nextHop == null) {
                logInfo("NO CANDIDATE FOUND; MESSAGE DISCARDED FOR VEHICLE DESTINATION: " + vehDest);
                return;
            }
            logDebug("NEXT HOP DETERMINED: " + nextHop);
        }
        // CREATE MESSAGE ROUTING
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        logDebug("ROUTING CREATED: CHANNEL CCH, TOPO BROADCAST MODE");
        // GENERATE UNIQUE R2V MESSAGE ID
        String messageId = "R2V-" + r2vCounter.incrementAndGet();
        logDebug("GENERATED R2V MESSAGE ID: " + messageId);
        // INITIALIZE FORWARDING TRAIL AND CREATE R2V MESSAGE
        List<String> forwardingTrail = new ArrayList<>();
        // NOTE: THE RSU SOURCE IS NOW PASSED AS THE CURRENT RSU ID
        R2VMsg r2vMsg = new R2VMsg(
                routing,
                messageId,
                f2rMsg.getTimeStamp(),
                f2rMsg.getTimestampLimit(),
                getOs().getId().toUpperCase(), // RSU SOURCE
                vehDest,
                nextHop,
                f2rMsg.getOrder(),
                forwardingTrail
        );
        logDebug("R2V MESSAGE CREATED WITH DETAILS: TIMESTAMP = " + f2rMsg.getTimeStamp() +
                ", TIMESTAMP_LIMIT = " + f2rMsg.getTimestampLimit() +
                ", ORDER = " + f2rMsg.getOrder() +
                ", VEHICLE_DEST = " + vehDest +
                ", NEXT_HOP = " + nextHop);
        // ADD THE R2V MESSAGE TO THE SENT ARCHIVE USING ITS UNIQUE ID AS THE KEY
        sentR2VArchive.put(r2vMsg.getUniqueId(), r2vMsg);
        // SEND THE R2V MESSAGE
        getOs().getAdHocModule().sendV2xMessage(r2vMsg);
        logInfo("R2V MESSAGE SENT (ID: " + messageId + ", NEXT_HOP: " + nextHop + ")");
    }

    /**
     * HANDLES V2VEXT MESSAGES TO UPDATE NEIGHBOR RECORDS.
     *
     * @param extMsg THE RECEIVED V2VEXT MESSAGE.
     */
    private void handleV2VExtMessage(V2VExt extMsg) {
        String senderId = extMsg.getSenderName().toUpperCase();
        double distance = computeDistance(getOs().getPosition(), extMsg.getSenderPos());
        long updateTime = extMsg.getTimeStamp();

        // VALIDATE THE INCOMING NEIGHBOR LIST; USE EMPTY LIST IF NULL
        List<String> incomingNeighbors = extMsg.getNeighborList();
        if (incomingNeighbors == null) {
            incomingNeighbors = new ArrayList<>();
            logDebug("NEIGHBOR LIST IS NULL FROM " + senderId + "; USING EMPTY LIST");
        }
        // CONVERT ALL NEIGHBORS TO UPPERCASE AND REMOVE DUPLICATES
        Set<String> neighborSet = new HashSet<>();
        for (String n : incomingNeighbors) {
            neighborSet.add(n.toUpperCase());
        }
        List<String> newNeighborList = new ArrayList<>(neighborSet);

        // CREATE A NEW RECORD FROM THE RECEIVED MESSAGE
        ExtendedNeighborRecord newRecord = new ExtendedNeighborRecord(senderId, distance, updateTime, newNeighborList);

        // MERGE OR CREATE THE NEIGHBOR RECORD
        ExtendedNeighborRecord existingRecord = neighborRecords.get(senderId);
        if (existingRecord != null) {
            if (updateTime > existingRecord.getLastUpdateTimestamp()) {
                // MERGE THE NEIGHBOR LISTS (UNION OF UNIQUE NEIGHBOR IDS)
                Set<String> mergedSet = new HashSet<>(existingRecord.getNeighborList());
                mergedSet.addAll(newNeighborList);
                existingRecord.setDistance(distance);
                existingRecord.setLastUpdateTimestamp(updateTime);
                existingRecord.setNeighborList(new ArrayList<>(mergedSet));
                logInfo("MERGED NEIGHBOR RECORD FOR " + senderId + ": " + existingRecord.toString());
            } else {
                logDebug("IGNORED OUTDATED DATA FOR " + senderId);
            }
        } else {
            neighborRecords.put(senderId, newRecord);
            logInfo("CREATED NEW NEIGHBOR RECORD FOR " + senderId + ": " + newRecord.toString());
        }
        logInfo("V2VEXT MESSAGE RECEIVED FROM " + senderId + " (DISTANCE: " + String.format("%.2f", distance) + " M)");
    }

    // ==================== HELPER METHODS ====================

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
     * REMOVES STALE NEIGHBOR RECORDS BASED ON THE SPECIFIED THRESHOLD.
     */
    private void removeStaleNeighbors() {
        long currentTime = getOs().getSimulationTime();
        // ITERATE OVER THE NEIGHBOR RECORDS AND REMOVE THOSE THAT ARE STALE
        Iterator<Map.Entry<String, ExtendedNeighborRecord>> iterator = neighborRecords.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ExtendedNeighborRecord> entry = iterator.next();
            long age = currentTime - entry.getValue().getLastUpdateTimestamp();
            // IF THE TIME SINCE LAST UPDATE IS GREATER THAN THE STALE THRESHOLD, REMOVE THE RECORD
            if (age > STALE_THRESHOLD) {
                logInfo("REMOVING STALE NEIGHBOR RECORD: " + entry.getKey() + " (AGE: " + age + " NS)");
                iterator.remove();
            }
        }
    }

    /**
     * DECIDES THE NEXT HOP FOR FORWARDING A R2V MESSAGE.
     *
     * @param vehDestination THE VEHICLE DESTINATION.
     * @return THE BEST NEXT HOP OR NULL IF NONE FOUND.
     */
    private String decideNextHop(String vehDestination) {
        logDebug("DECIDING NEXT HOP FOR VEHICLE DESTINATION: " + vehDestination);
        if (neighborRecords.containsKey(vehDestination)) {
            logDebug("VEHICLE DESTINATION DIRECTLY REACHABLE: " + vehDestination);
            return vehDestination;
        }
        int bestScore = Integer.MIN_VALUE;
        String bestCandidate = null;
        // EVALUATE EACH NEIGHBOR RECORD
        for (ExtendedNeighborRecord record : neighborRecords.values()) {
            int hops = getMinHopCount(record.getId(), vehDestination);
            if (hops == Integer.MAX_VALUE) continue;
            int connectivity = record.getNeighborList().size();
            int score = connectivity - hops;
            logDebug("EVALUATING NEIGHBOR: " + record.getId() + " | HOPS = " + hops +
                     " | CONNECTIVITY = " + connectivity + " | SCORE = " + score);
            if (score > bestScore) {
                bestScore = score;
                bestCandidate = record.getId();
            }
        }
        if (bestCandidate != null) {
            logDebug("BEST CANDIDATE SELECTED: " + bestCandidate + " WITH SCORE: " + bestScore);
        } else {
            logDebug("NO CANDIDATE FOUND FOR VEHICLE DESTINATION: " + vehDestination);
        }
        return bestCandidate;
    }

    /**
     * CALCULATES THE MINIMUM HOP COUNT BETWEEN TWO VEHICLES USING BFS.
     *
     * @param start       THE STARTING VEHICLE.
     * @param destination THE DESTINATION VEHICLE.
     * @return THE MINIMUM NUMBER OF HOPS, OR Integer.MAX_VALUE IF NOT REACHABLE.
     */
    private int getMinHopCount(String start, String destination) {
        logDebug("CALCULATING MIN HOP COUNT FROM " + start + " TO " + destination);
        if (start.equalsIgnoreCase(destination)) return 0;
        Queue<String> queue = new LinkedList<>();
        Map<String, Integer> distanceMap = new HashMap<>();
        queue.offer(start);
        distanceMap.put(start, 0);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            int hops = distanceMap.get(current);
            if (current.equalsIgnoreCase(destination)) {
                logDebug("DESTINATION " + destination + " REACHED WITH HOP COUNT: " + hops);
                return hops;
            }
            ExtendedNeighborRecord record = neighborRecords.get(current);
            if (record != null && record.getNeighborList() != null) {
                for (String neighbor : record.getNeighborList()) {
                    if (!distanceMap.containsKey(neighbor)) {
                        distanceMap.put(neighbor, hops + 1);
                        queue.offer(neighbor);
                        logDebug("VISITING NEIGHBOR: " + neighbor + " WITH HOP COUNT: " + (hops + 1));
                    }
                }
            }
        }
        logDebug("DESTINATION " + destination + " NOT REACHABLE FROM " + start);
        return Integer.MAX_VALUE;
    }

    /**
     * PRINTS THE CURRENT NEIGHBOR RECORDS.
     */
    private void printNeighborRecords() {
        StringBuilder sb = new StringBuilder();
        sb.append("[RSU APP] NEIGHBOR RECORDS:\n");
        for (Map.Entry<String, ExtendedNeighborRecord> entry : neighborRecords.entrySet()) {
            ExtendedNeighborRecord rec = entry.getValue();
            sb.append("  ").append(rec.getId().toUpperCase()).append(" -> DIST: ")
              .append(String.format("%.2f", rec.getDistance()))
              .append(" M, LAST_UPDATE: ").append(rec.getLastUpdateTimestamp())
              .append(", NEIGHBORS: ").append(rec.getNeighborList().toString().toUpperCase())
              .append("\n");
        }
        logInfo(sb.toString());
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