package pt.uminho.npr.tutorial;

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
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FogApp extends AbstractApplication<RoadSideUnitOperatingSystem>
                   implements CommunicationApplication {

    private final long MSG_DELAY = 1000 * TIME.MILLI_SECOND;
    private final int TX_POWER = 70;
    private final double TX_RANGE = 1500.0;
    private static final long RSU_TIMEOUT = 60000;

    private static class RsuRecord {
        long lastHeardTime;
        GeoPoint position;
    }

    private final Map<String, RsuRecord> connectedRsus = new HashMap<>();

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER)
                .distance(TX_RANGE)
                .create());
        getLog().infoSimTime(this, "FOGAPP ON STARTUP");
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        getLog().infoSimTime(this, "FOGAPP ON SHUTDOWN");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) throws Exception {
        printRsuList();
        cleanInactiveRsus();
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    private void printRsuList() {
        StringBuilder sb = new StringBuilder("FOGAPP RSUs CONNECTED: [");
        for (Map.Entry<String, RsuRecord> e : connectedRsus.entrySet()) {
            sb.append(e.getKey()).append("(pos=").append(e.getValue().position).append(") ");
        }
        sb.append("]");
        getLog().infoSimTime(this, sb.toString());
    }

    private void cleanInactiveRsus() {
        long now = getOs().getSimulationTime();
        Iterator<Map.Entry<String, RsuRecord>> it = connectedRsus.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RsuRecord> entry = it.next();
            if ((now - entry.getValue().lastHeardTime) > RSU_TIMEOUT) {
                getLog().infoSimTime(this, "FOGAPP REMOVING INACTIVE RSU: " + entry.getKey());
                it.remove();
            }
        }
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rx) {
        V2xMessage msg = rx.getMessage();
        if (msg instanceof RsuFogMsg) {
            RsuFogMsg fmsg = (RsuFogMsg) msg;
            if ("FogNode".equals(fmsg.getDestNode())) {
                handleRsuFogMsg(fmsg);
            } else {
                getLog().infoSimTime(this, "FOGAPP IGNORED RsuFogMsg not for me: " + fmsg);
            }
        } else {
            getLog().infoSimTime(this, "FOGAPP IGNORED MSG: " + msg);
        }
    }

    private void handleRsuFogMsg(RsuFogMsg fmsg) {
        String rid = fmsg.getRsuName();
        getLog().infoSimTime(this, "FOGAPP RECEIVED RSUFOGMSG FROM " + rid);
        RsuRecord rec = connectedRsus.get(rid);
        if (rec == null) {
            rec = new RsuRecord();
            connectedRsus.put(rid, rec);
        }
        rec.lastHeardTime = fmsg.getTimeStamp();
        rec.position = fmsg.getRsuPosition();
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        getLog().infoSimTime(this, "FOGAPP ON MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        getLog().infoSimTime(this, "FOGAPP ON ACK RECEIVED: " + ack);
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        getLog().infoSimTime(this, "FOGAPP ON CAM BUILDING");
    }
}