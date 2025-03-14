package pt.uminho.npr.tutorial;

import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleData;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VehApp extends AbstractApplication<VehicleOperatingSystem> implements VehicleApplication, CommunicationApplication {
    private final long MSG_DELAY = 200 * TIME.MILLI_SECOND;
    private final int TX_POWER = 50;
    private final double TX_RANGE = 500.0;
    private boolean startSending = false;
    private double vehHeading;
    private double vehSpeed;
    private int vehLane;

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER)
                .distance(TX_RANGE)
                .create());
        getLog().infoSimTime(this, "VEHAPP ON STARTUP: VEHICLE APPLICATION INITIALIZED");
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    @Override
    public void onShutdown() {
        getLog().infoSimTime(this, "VEHAPP ON SHUTDOWN: VEHICLE APPLICATION TERMINATED");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void onVehicleUpdated(@Nullable VehicleData previousVehicleData, @Nonnull VehicleData updatedVehicleData) {
        vehHeading = updatedVehicleData.getHeading().doubleValue();
        vehSpeed = updatedVehicleData.getSpeed();
        vehLane = updatedVehicleData.getRoadPosition().getLaneIndex();
        if (!startSending) {
            startSending = true;
            getLog().infoSimTime(this, "VEHAPP: READY TO SEND VEHICLE INFORMATION MESSAGES");
        }
    }

    @Override
    public void processEvent(Event event) {
        if (startSending) {
            sendVehInfoMsg();
        }
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + MSG_DELAY, this);
    }

    private void sendVehInfoMsg() {
        MessageRouting routing = getOs().getAdHocModule().createMessageRouting()
                .viaChannel(AdHocChannel.CCH)
                .topoBroadCast();
        long time = getOs().getSimulationTime();
        VehInfoMsg msg = new VehInfoMsg(routing, time, getOs().getId(), getOs().getPosition(), vehHeading, vehSpeed, vehLane);
        getOs().getAdHocModule().sendV2xMessage(msg);
        getLog().infoSimTime(this, "VEHAPP SENT VEHINFOMSG: " + msg.toString() + " AT SIMULATION TIME " + time);
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage rcvMsg) {
        getLog().infoSimTime(this, "VEHAPP ON MESSAGE RECEIVED: " + rcvMsg.getMessage().toString());
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        getLog().infoSimTime(this, "VEHAPP ON MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        getLog().infoSimTime(this, "VEHAPP ON ACKNOWLEDGEMENT RECEIVED");
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        getLog().infoSimTime(this, "VEHAPP ON CAM BUILDING");
    }
}