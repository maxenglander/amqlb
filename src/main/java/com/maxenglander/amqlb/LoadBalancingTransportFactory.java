package com.maxenglander.amqlb;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author maxenglander
 */
public class LoadBalancingTransportFactory extends TransportFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadBalancingTransport.class);
        
    @Override
    public Transport configure(Transport transport, WireFormat wf, Map options) throws Exception {
        LOGGER.debug("configure()");
        return super.configure(transport, wf, options);
    }
    
    @Override
    public Transport doConnect(URI location) throws Exception {
        CompositeData compositeData = URISupport.parseComposite(location);
        URI[] uris = compositeData.getComponents();
        Transport[] transports = new Transport[uris.length];
        for(int i = 0; i < uris.length; i++) {
            LOGGER.debug("Connecting transport {}, uri={}", i, uris[i].toASCIIString());
            transports[i] = TransportFactory.compositeConnect(uris[i]);
        }
        
        Transport transport = new LoadBalancingTransport(transports);
        return new MutexTransport(new ResponseCorrelator(transport));
    }
    
    @Override
    public TransportServer doBind(URI uri) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
