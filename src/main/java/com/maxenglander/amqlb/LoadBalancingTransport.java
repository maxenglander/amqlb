package com.maxenglander.amqlb;

import java.io.IOException;
import java.net.URI;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author maxenglander
 * 
 * Load balance requests among multiple transports.
 */
public class LoadBalancingTransport implements Transport {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadBalancingTransport.class);
    
private final CircularCounter counter;    
    private final Transport[] transports;    
    
    private TransportListener transportListener;    
    
    public LoadBalancingTransport(Transport... transports) {
        this.counter = new CircularCounter(0, transports.length - 1);      
        this.transports = transports;        
    }
    
    public void oneway(Object o) throws IOException {        
        Command command = (Command)o;
        if(command.isMessage()) {            
            final int index = counter.getAndIncrement();
            LOGGER.debug("Sending message to transport {}", index);
            transports[index].oneway(o);
        } else {
            for(Transport transport : transports) {
                transport.oneway(o);
            }
        }
    }

    public FutureResponse asyncRequest(Object o, ResponseCallback rc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Object request(Object o) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Object request(Object o, int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public TransportListener getTransportListener() {
        LOGGER.debug("getTransportListener()");
        return transportListener;
    }

    public void setTransportListener(TransportListener tl) {
        LOGGER.debug("setTransportListener()");
        this.transportListener = tl;
        for(Transport transport : transports) {
            transport.setTransportListener(tl);
        }
    }

    public <T> T narrow(Class<T> type) {
        LOGGER.debug("narrow()");
        return null;
    }

    public String getRemoteAddress() {
        LOGGER.debug("getRemoteAddress()");
        return null;
    }

    public boolean isFaultTolerant() {
        LOGGER.debug("isFaultTolerant()");
        return false;
    }

    public boolean isDisposed() {
        LOGGER.debug("isDisposed()");
        return false;
    }

    public boolean isConnected() {
        LOGGER.debug("isConnected()");
        for(Transport transport : transports) {
            if(!transport.isConnected()) {
                return false;
            }
        }
        return true;
    }

    public boolean isReconnectSupported() {
        LOGGER.debug("isReconnectSupported()");
        return true;
    }

    public boolean isUpdateURIsSupported() {
        LOGGER.debug("isUpdateURIsSupported()");
        return false;
    }

    public void reconnect(URI uri) throws IOException {        
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateURIs(boolean bln, URI[] uris) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int getReceiveCounter() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void start() throws Exception {  
        int index = 0;
        for(Transport transport : transports) {            
            LOGGER.debug("Starting transport {}", index);
            transport.start();
            index++;
        }
    }

    public void stop() throws Exception {
        LOGGER.debug("stop()");
        for(Transport transport : transports) {
            transport.stop();
        }
    }
}