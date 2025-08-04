package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.app.api.os.ServerOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CellModuleConfiguration;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.DATA;
import org.eclipse.mosaic.rti.TIME;

import pt.uminho.npr.projeto.messages.EventMessage;
import pt.uminho.npr.projeto.messages.EventACK;
import pt.uminho.npr.projeto.messages.AccidentEvent;
import pt.uminho.npr.projeto.messages.CamMessage;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public final class FogApp extends AbstractApplication<ServerOperatingSystem>
        implements CommunicationApplication {

    private static final long   TICK_INTERVAL = 500 * TIME.MILLI_SECOND;
    private static final long   CAM_TTL       = 2 * TIME.SECOND;
    private static final long   EVENT_TTL     = 8 * TIME.SECOND;
    //private static final double EVENT_PROB    = 0.02;

    private static final Map<String, GeoPoint> STATIC_RSUS = Map.ofEntries(
        Map.entry("rsu_0",  GeoPoint.latLon(52.451033, 13.295327, 0)),
        Map.entry("rsu_1",  GeoPoint.latLon(52.451406, 13.298062, 0)),
        Map.entry("rsu_2",  GeoPoint.latLon(52.452119, 13.301154, 0)),
        Map.entry("rsu_3",  GeoPoint.latLon(52.452153, 13.304256, 0)),
        Map.entry("rsu_4",  GeoPoint.latLon(52.450304, 13.301368, 0)),
        Map.entry("rsu_5",  GeoPoint.latLon(52.450268, 13.304328, 0)),
        Map.entry("rsu_6",  GeoPoint.latLon(52.448599, 13.305743, 0)),
        Map.entry("rsu_7",  GeoPoint.latLon(52.448538, 13.302883, 0)),
        Map.entry("rsu_8",  GeoPoint.latLon(52.447641, 13.301310, 0)),
        Map.entry("rsu_9",  GeoPoint.latLon(52.449290, 13.298481, 0)),
        Map.entry("rsu_10", GeoPoint.latLon(52.446686, 13.298421, 0)),
        Map.entry("rsu_11", GeoPoint.latLon(52.446557, 13.294261, 0)),
        Map.entry("rsu_12", GeoPoint.latLon(52.448404, 13.295637, 0)),
        Map.entry("rsu_13", GeoPoint.latLon(52.449206, 13.292616, 0))
    );

    private final Map<String, CamMessage> seenCams = new HashMap<>();
    private final Map<Integer, String> openEvents = new HashMap<>();
    private final AtomicInteger eventSeq = new AtomicInteger();
    private final Random random = new Random();

    @Override
    public void onStartup() {
        
        // Enable communication module 
        getOs().getCellModule().enable(new CellModuleConfiguration()
            .maxDownlinkBitrate(50 * DATA.MEGABIT)
            .maxUplinkBitrate(50 * DATA.MEGABIT)
        );
        scheduleEvent();
        logInfo("FOG_INITIALIZATION");
    }

    @Override
    public void onShutdown() {
        logLostEvents();
        getOs().getCellModule().disable();
        logInfo("FOG_SHUTDOWN");
    }

    private void logLostEvents() {
        if (openEvents.isEmpty()) {
            logInfo("NO LOST EVENTS");
            return;
        }
        StringBuilder sb = new StringBuilder(String.format("LOST EVENTS [%d]: ", openEvents.size()));
        for (Entry<Integer, String> entry : openEvents.entrySet()) {
            sb.append(entry.getKey()).append(" (").append(entry.getValue()).append("), ");
        }
        logInfo(sb.substring(0, sb.length() - 2));
    }
    
    private void scheduleEvent() {
        getOs().getEventManager().addEvent(
            getOs().getSimulationTime() + TICK_INTERVAL,
            this
        );
    }
    
    @Override
    public void onMessageReceived(ReceivedV2xMessage incoming) {
        V2xMessage msg = incoming.getMessage();
        
        if (msg instanceof CamMessage cam && 
            (!seenCams.containsKey(cam.getVehId()) || cam.getId() > seenCams.get(cam.getVehId()).getId())
        ) {
            // Store the last cam message for the vehicle
            seenCams.put(cam.getVehId(), cam);

        } else if (msg instanceof EventACK ack) {
            logInfo(String.format(
                "ACK_RECEIVED : EVENT_ID: %d",
                ack.getId()
            ));
            handleAckReceived(ack);
        }
    }

    private void handleAckReceived(EventACK ack) {
                    
        // Check if the ACK followed the correct trail
        if(!ack.getChecklist().isEmpty()) {
            logInfo(String.format(
                "ACK_ERROR : ACK_ID: %d | CHECKLIST_SIZE: %d",
                ack.getId(), ack.getChecklist().size()
            ));
        }

        // Remove the event from the open events list
        openEvents.remove(ack.getId()); 
        logInfo(String.format(
            "EVENT_CLOSED : EVENT_ID: %d",
            ack.getId()
        ));
    }

    @Override
    public void processEvent(Event event) {
        purgeCams();
        maybeGenerateEvent();
        scheduleEvent();
    }

    private void purgeCams() {
        long now = getOs().getSimulationTime();
        seenCams.entrySet().removeIf(
            e -> now - e.getValue().getTimestamp() > CAM_TTL
        );
    }

    private void maybeGenerateEvent() {
        if (seenCams.size() < 2) { // Precisa de pelo menos 2 veículos para colisão
            return;
        }

        long now = getOs().getSimulationTime();
        List<CamMessage> vehicles = new ArrayList<>(seenCams.values());

        // Analisar pares de veículos
        // Melhoria Futura: Usar algoritmo de detecção de colisão mais eficiente
        for (int i = 0; i < vehicles.size(); i++) {
            for (int j = i + 1; j < vehicles.size(); j++) {
                CamMessage vehA = vehicles.get(i);
                CamMessage vehB = vehicles.get(j);

                if (openEvents.containsValue(vehA.getVehId()) || openEvents.containsValue(vehB.getVehId())) {
                    continue;
                }

                // Calcular distância
                double distance = vehA.getPosition().distanceTo(vehB.getPosition());
                if (distance > 100) { // Ignorar veículos muito distantes
                    continue;
                }

                // Calcular velocidade relativa
                double speedA = vehA.getSpeed();
                double speedB = vehB.getSpeed();
                double headingA = Math.toRadians(vehA.getHeading());
                double headingB = Math.toRadians(vehB.getHeading());

                // Componentes de velocidade
                double vAx = speedA * Math.cos(headingA);
                double vAy = speedA * Math.sin(headingA);
                double vBx = speedB * Math.cos(headingB);
                double vBy = speedB * Math.sin(headingB);
                double relativeSpeed = Math.sqrt(Math.pow(vAx - vBx, 2) + Math.pow(vAy - vBy, 2));

                // Calcular TTC (considerando direção relativa)
                double deltaX = vehB.getPosition().getLongitude() - vehA.getPosition().getLongitude();
                double deltaY = vehB.getPosition().getLatitude() - vehA.getPosition().getLatitude();
                double relativeSpeedAlongPath = ((vBx - vAx) * deltaX + (vBy - vAy) * deltaY) / distance;

                // Evitar TTC inflado por velocidade relativa muito pequena
                double ttc;
                if (relativeSpeedAlongPath > 0.01) { // Limiar mínimo para evitar TTCs enormes
                    ttc = distance / relativeSpeedAlongPath;
                } else if (relativeSpeed > 0.01) { // Usar velocidade relativa total como fallback
                    ttc = distance / relativeSpeed;
                } else {
                    ttc = Double.POSITIVE_INFINITY;
                }

                // Log para depuração
                logInfo(String.format(
                    "DEBUG: VEH_A: %s | VEH_B: %s | speedA: %.2f m/s | speedB: %.2f m/s | headingA: %.2f deg | headingB: %.2f deg | relativeSpeed: %.4f m/s | relativeSpeedAlongPath: %.4f m/s",
                    vehA.getVehId(), vehB.getVehId(), speedA, speedB, vehA.getHeading(), vehB.getHeading(), relativeSpeed, relativeSpeedAlongPath
                ));

                // Determinar severidade com base em TTC e distância
                int severity = -1;
                if (distance < 5.0) { // Critério de distância fixa para travagem
                    severity = 2; // Grave: proximidade crítica
                } else if (ttc < 1.5) { // Grave: colisão iminente
                    severity = 2;
                } else if (ttc < 2.5) { // Moderado
                    severity = 1;
                } else if (ttc < 4.0) { // Leve
                    severity = 0;
                }

                if (severity >= 0) {
                    logInfo(String.format(
                        "COLLISION_RISK_DETECTED : VEH_A: %s | VEH_B: %s | TTC: %.2f | DISTANCE: %.2f | SEVERITY: %d",
                        vehA.getVehId(), vehB.getVehId(), ttc, distance, severity
                    ));
                    generateEventForVehicle(vehA.getVehId(), vehA.getPosition(), now, severity);
                    generateEventForVehicle(vehB.getVehId(), vehB.getPosition(), now, severity);
                } else {
                    logInfo(String.format(
                        "NO_RISK : VEH_A: %s | VEH_B: %s | TTC: %.2f | DISTANCE: %.2f",
                        vehA.getVehId(), vehB.getVehId(), ttc, distance
                    ));
                }
            }
        }
    }

    private void generateEventForVehicle(String target, GeoPoint position, long now, int severity) {
        int id = eventSeq.getAndIncrement();
        String rsuId = getClosestRsu(position);
        logInfo(String.format("EVENT_ROUTING : ID: %s | RSU: %s", id, rsuId));

        EventMessage event = new AccidentEvent(
            newRouting(rsuId),
            id,
            now,
            now + EVENT_TTL,
            target,
            new ArrayList<>(List.of(rsuId)),
            severity
        );

        getOs().getCellModule().sendV2xMessage(event);
        openEvents.put(id, target); 
        logInfo(String.format("EVENT GENERATED AND SENT : UNIQUE_ID: %d | TARGET: %s | SEVERITY: %d", id, target, severity));
    }   

    private MessageRouting newRouting(String rsuId) {
    
        // Create a new routing for the event message
        return getOs().getCellModule()
            .createMessageRouting()
            .destination(rsuId)
            .topological()
            .build();
    }

    private String getClosestRsu(GeoPoint position) {
        String closest = null;
        double minDistance = Double.MAX_VALUE;

        for (Entry<String,GeoPoint> rsu : STATIC_RSUS.entrySet()) {
            double distance = rsu.getValue().distanceTo(position);
            if(distance < minDistance) {
                minDistance = distance;
                closest = rsu.getKey();
            }
        }
        return closest;
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { /* No action required */ }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { /* No action required */ }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { /* No action required */ }

    private void logInfo(String message) {
        getLog().infoSimTime(this, "[INFO]  " + message);
    }
}