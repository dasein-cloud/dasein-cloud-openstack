/**
 * Copyright (C) 2009-2015 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.openstack.nova.os.ext.rackspace.lb;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.network.*;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;


public class RackspaceLoadBalancers extends AbstractLoadBalancerSupport<NovaOpenStack> {
    static private final Logger logger = NovaOpenStack.getLogger(RackspaceLoadBalancers.class, "std");

    static public final String RESOURCE = "/loadbalancers";
    static public final String SERVICE  = "rax:load-balancer";

    private NovaOpenStack provider;
    
    public RackspaceLoadBalancers(NovaOpenStack provider) {
        super(provider);
        this.provider = provider;
    }

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return getContext().getAccountNumber();
    }

    public void addIPEndpoints(@Nonnull String toLoadBalancerId, @Nonnull String ... ipAddresses) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.addIPEndpoints");
        try {
            List<Map<String,Object>> nodes = new ArrayList<>();
            LoadBalancer lb = getLoadBalancer(toLoadBalancerId);
            int port = -1;

            if( lb == null ) {
                logger.error("No such load balancer: " + toLoadBalancerId);
                throw new GeneralCloudException("No such load balancer: " + toLoadBalancerId, CloudErrorType.GENERAL);
            }
            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 5L);

            while( timeout > System.currentTimeMillis() ) {
                if( lb == null ) {
                    return;
                }
                if( !LoadBalancerState.PENDING.equals(lb.getCurrentState()) ) {
                    break;
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
                try {
                    lb = getLoadBalancer(toLoadBalancerId);
                    if( lb == null ) {
                        return;
                    }
                    if( !LoadBalancerState.PENDING.equals(lb.getCurrentState()) ) {
                        break;
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }
            LbListener[] listeners = lb.getListeners();

            if( listeners != null && listeners.length > 0 ) {
                port = listeners[0].getPrivatePort();
                if( port == -1 ) {
                    port = listeners[0].getPublicPort();
                }
            }
            if( port == -1 ) {
                if( lb.getPublicPorts() != null && lb.getPublicPorts().length > 0 ) {
                    port = lb.getPublicPorts()[0];
                }
                if( port == -1 ) {
                    logger.error("Could not determine a proper private port for mapping");
                    throw new GeneralCloudException("No port understanding exists for this load balancer", CloudErrorType.GENERAL);
                }
            }
            for( String address : ipAddresses ) {
                if( logger.isTraceEnabled() ) {
                    logger.trace("Adding " + address + "...");
                }
                Map<String,Object> node = new HashMap<>();


                node.put("address", address);
                node.put("condition", "ENABLED");
                node.put("port", port);
                nodes.add(node);
            }
            if( !nodes.isEmpty() ) {
                Map<String,Object> json = new HashMap<>();

                json.put("nodes", nodes);
                NovaMethod method = new NovaMethod(provider);

                if( logger.isTraceEnabled() ) {
                    logger.trace("Calling cloud...");
                }
                method.postString(SERVICE, RESOURCE, toLoadBalancerId + "/nodes", new JSONObject(json), false);
                if( logger.isTraceEnabled() ) {
                    logger.trace("Done.");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void addServers(@Nonnull String toLoadBalancerId, @Nonnull String... serverIdsToAdd) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.addServers");
        try {
            List<Map<String,Object>> nodes = new ArrayList<>();
            LoadBalancer lb = getLoadBalancer(toLoadBalancerId);
            int port = -1;
            
            if( lb == null ) {
                logger.error("addServers(): No such load balancer: " + toLoadBalancerId);
                throw new GeneralCloudException("No such load balancer: " + toLoadBalancerId, CloudErrorType.GENERAL);
            }
            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 5L);

            while( timeout > System.currentTimeMillis() ) {
                if( lb == null ) {
                    return;
                }
                if( !LoadBalancerState.PENDING.equals(lb.getCurrentState()) ) {
                    break;
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
                try {
                    lb = getLoadBalancer(toLoadBalancerId);
                    if( lb == null ) {
                        return;
                    }
                    if( !LoadBalancerState.PENDING.equals(lb.getCurrentState()) ) {
                        break;
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }
            LbListener[] listeners = lb.getListeners();
            
            if( listeners != null && listeners.length > 0 ) {
                port = listeners[0].getPrivatePort();
                if( port == -1 ) {
                    port = listeners[0].getPublicPort();
                }
            }
            if( port == -1 ) {
                if( lb.getPublicPorts() != null && lb.getPublicPorts().length > 0 ) {
                    port = lb.getPublicPorts()[0];
                }
                if( port == -1 ) {
                    logger.error("addServers(): Could not determine a proper private port for mapping");
                    throw new GeneralCloudException("No port mapping exists for this load balancer", CloudErrorType.GENERAL);
                }
            }
            for( String id : serverIdsToAdd ) {
                if( logger.isTraceEnabled() ) {
                    logger.trace("addServers(): Adding " + id + "...");
                }
                VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(id);
                
                if( vm == null ) {
                    logger.error("addServers(): Failed to add " + id + " because it does not exist");
                    throw new ResourceNotFoundException("No such server: " + id);
                }
                String address = null;
                
                if( vm.getProviderRegionId().equals(getContext().getRegionId()) ) {
                    RawAddress[] possibles = vm.getPrivateAddresses();
                    address = ((possibles != null && possibles.length > 0) ? possibles[0].getIpAddress() : null);

                }
                if( address == null ) {
                    RawAddress[] possibles = vm.getPublicAddresses();
                        
                    address = ((possibles != null && possibles.length > 0) ? possibles[0].getIpAddress() : null);
                }
                if( address == null ) {
                    logger.error("addServers(): No address exists for mapping the load balancer to this server");
                    throw new ResourceNotFoundException("The virtual machine " + id + " has no mappable addresses");
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("addServers(): Mapping IP is: " + address);
                }
                Map<String,Object> node = new HashMap<>();
                
                
                node.put("address", address);
                node.put("condition", "ENABLED");
                node.put("port", port);
                nodes.add(node);
            }
            if( !nodes.isEmpty() ) {
                Map<String,Object> json = new HashMap<>();
            
                json.put("nodes", nodes);
                NovaMethod method = new NovaMethod(provider);
                
                if( logger.isTraceEnabled() ) {
                    logger.debug("addServers(): Calling cloud...");
                }
                method.postString(SERVICE, RESOURCE, toLoadBalancerId + "/nodes", new JSONObject(json), false);
                if( logger.isTraceEnabled() ) {
                    logger.debug("addServers(): Done.");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createLoadBalancer(@Nonnull LoadBalancerCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.create");
        try {
            LbListener[] listeners = options.getListeners();

            if( listeners == null || listeners.length < 1 ) {
                logger.error("create(): Call failed to specify any listeners");
                throw new InternalException("Rackspace requires exactly one listener");
            }
            Map<String,Object> lb = new HashMap<>();
            
            lb.put("name", options.getName());
            lb.put("port", listeners[0].getPublicPort());
            if( listeners[0].getNetworkProtocol().equals(LbProtocol.HTTP) ) {
                lb.put("protocol", "HTTP");
            }
            else if( listeners[0].getNetworkProtocol().equals(LbProtocol.HTTPS) ) {
                lb.put("protocol", "HTTPS");
            }
            else if( listeners[0].getNetworkProtocol().equals(LbProtocol.RAW_TCP) ) {
                lb.put("protocol", matchProtocol(listeners[0].getPublicPort()));
            }
            else {
                logger.error("Invalid protocol: " + listeners[0].getNetworkProtocol());
                throw new InternalException("Unsupported protocol: " + listeners[0].getNetworkProtocol());
            }
            if( listeners[0].getAlgorithm().equals(LbAlgorithm.LEAST_CONN) ) {
                lb.put("algorithm", "LEAST_CONNECTIONS");
            }
            else if( listeners[0].getAlgorithm().equals(LbAlgorithm.ROUND_ROBIN) ) {
                lb.put("algorithm", "ROUND_ROBIN");
            }
            else {
                logger.error("create(): Invalid algorithm: " + listeners[0].getAlgorithm());
                throw new InternalException("Unsupported algorithm: " + listeners[0].getAlgorithm());
            }
            List<Map<String,Object>> ips = new ArrayList<>();
            Map<String,Object> ip = new HashMap<>();
            
            ip.put("type", "PUBLIC");
            ips.add(ip);
            lb.put("virtualIps", ips);
            
            List<Map<String,Object>> nodes = new ArrayList<>();
            LoadBalancerEndpoint[] endpoints = options.getEndpoints();

            if( endpoints != null ) {
                Set<String> addresses = new TreeSet<String>();

                for( LoadBalancerEndpoint endpoint : endpoints ) {
                    String address = null;

                    if( endpoint.getEndpointType().equals(LbEndpointType.IP) ) {
                        address = endpoint.getEndpointValue();
                    }
                    else {
                        VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(endpoint.getEndpointValue());

                        if( vm != null ) {
                            if( vm.getProviderRegionId().equals(getContext().getRegionId()) ) {
                                RawAddress[] tmp = vm.getPrivateAddresses();

                                if( tmp != null && tmp.length > 0 ) {
                                    address = tmp[0].getIpAddress();
                                }
                            }
                            if( address == null ) {
                                RawAddress[] tmp = vm.getPublicAddresses();

                                if( tmp != null && tmp.length > 0 ) {
                                    address = tmp[0].getIpAddress();
                                }
                            }
                        }
                    }
                    if( address != null && !addresses.contains(address) ) {
                        Map<String,Object> node = new HashMap<>();

                        node.put("address", address);
                        node.put("condition", "ENABLED");
                        node.put("port", listeners[0].getPrivatePort());
                        nodes.add(node);
                        addresses.add(address);
                    }
                }
            }
            if( nodes.isEmpty() ) {
                logger.error("create(): Rackspace requires at least one node assignment");
                throw new InternalException("Rackspace requires at least one node assignment");
            }
            lb.put("nodes", nodes);
            
            Map<String,Object> json = new HashMap<>();
            
            json.put("loadBalancer", lb);
            NovaMethod method = new NovaMethod(provider);
            
            if( logger.isTraceEnabled() ) {
                logger.trace("create(): Posting new load balancer data...");
            }
            JSONObject result = method.postString(SERVICE, RESOURCE, null, new JSONObject(json), false);
            
            if( result == null ) {
                logger.error("create(): Method executed successfully, but no load balancer was created");
                throw new GeneralCloudException("Method executed successfully, but no load balancer was created", CloudErrorType.GENERAL);
            }
            try{
                if( result.has("loadBalancer") ) {
                    JSONObject ob = result.getJSONObject("loadBalancer");
                    
                    if( ob != null ) {
                        return ob.getString("id");
                    }
                }
                logger.error("create(): Method executed successfully, but no load balancer was found in JSON");                        
                throw new CommunicationException("Method executed successfully, but no load balancer was found in JSON");
            }
            catch( JSONException e ) {
                logger.error("create(): Failed to identify a load balancer ID in the cloud response: " + e.getMessage());
                throw new CommunicationException("Failed to identify a load balancer ID in the cloud response: " + e.getMessage(), e);

            }
        }
        finally {
            APITrace.end();
        }
    }

    private String matchProtocol(int port) throws CloudException, InternalException {
        NovaMethod method = new NovaMethod(provider);
        JSONObject ob = method.getResource(SERVICE, RESOURCE, "protocols", false);
        
        if( ob == null ) {
            return "TCP";
        }
        else {
            if( ob.has("protocols") ) {
                try {
                    JSONArray list = ob.getJSONArray("protocols");
                    
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject p = list.getJSONObject(i);
                        
                        if( p.has("port") && p.getInt("port") == port ) {
                            return p.getString("name");
                        }
                    }
                }
                catch( JSONException e ) {
                    throw new CommunicationException("Unable to parse protocols from Rackspace: " + e.getMessage(), e);

                }
            }
            return "TCP";
        }
    }

    @Override
    public @Nullable LoadBalancer getLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getLoadBalancer");
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getResource(SERVICE, RESOURCE, loadBalancerId, false);
            
            if( ob == null ) {
                return null;
            }
            Iterable<VirtualMachine> vms = provider.getComputeServices().getVirtualMachineSupport().listVirtualMachines();
            
            try {
                if( ob.has("loadBalancer") ) {
                    LoadBalancer lb = toLoadBalancer(ob.getJSONObject("loadBalancer"), vms);
                        
                    if( lb != null ) {
                        return lb;
                    }
                }
                return null;
            }
            catch( JSONException e ) {
                logger.error("listLoadBalancers(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to identify expected values in JSON" + e.getMessage(), e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile RackspaceLBCapabilities capabilities;
    @Nonnull
    @Override
    public LoadBalancerCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new RackspaceLBCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listEndpoints");
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getResource(SERVICE, RESOURCE, loadBalancerId, false);

            if( ob == null ) {
                return Collections.emptyList();
            }
            try {
                if( ob.has("loadBalancer") && !ob.isNull("loadBalancer") ) {
                    JSONObject json = ob.getJSONObject("loadBalancer");

                    if( json.has("nodes") ) {
                        List<LoadBalancerEndpoint> endpoints = new ArrayList<>();
                        JSONArray arr = json.getJSONArray("nodes");

                        for( int i=0; i<arr.length(); i++ ) {
                            LbEndpointState state = LbEndpointState.ACTIVE;
                            JSONObject item = arr.getJSONObject(i);

                            if( item.has("condition") && !item.getString("condition").equalsIgnoreCase("enabled") ) {
                                state = LbEndpointState.INACTIVE;
                            }
                            if( item.has("address") && !item.isNull("address")) {
                                String addr = item.getString("address");
                                VirtualMachine node = null;

                                for( VirtualMachine vm : getProvider().getComputeServices().getVirtualMachineSupport().listVirtualMachines() ) {
                                    RawAddress[] addrs = vm.getPublicAddresses();

                                    for( RawAddress a : addrs ){
                                        if( addr.equals(a.getIpAddress()) ) {
                                            node = vm;
                                            break;
                                        }
                                    }
                                    if( node == null ) {
                                        addrs = vm.getPrivateAddresses();
                                        for( RawAddress a : addrs ){
                                            if( addr.equals(a.getIpAddress()) ) {
                                                node = vm;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if( node != null ) {
                                    endpoints.add(LoadBalancerEndpoint.getInstance(LbEndpointType.VM, node.getProviderVirtualMachineId(), state));
                                }
                                else {
                                    endpoints.add(LoadBalancerEndpoint.getInstance(LbEndpointType.IP, addr, state));
                                }
                            }
                        }
                        return endpoints;
                    }
                }
                return Collections.emptyList();
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to identify expected values in JSON" + e.getMessage(), e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints(@Nonnull String loadBalancerId, @Nonnull LbEndpointType type, @Nonnull String ... values) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listEndpoints");
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getResource(SERVICE, RESOURCE, loadBalancerId, false);

            if( ob == null ) {
                return Collections.emptyList();
            }
            try {
                if( ob.has("loadBalancer") && !ob.isNull("loadBalancer") ) {
                    JSONObject json = ob.getJSONObject("loadBalancer");

                    if( json.has("nodes") ) {
                        List<LoadBalancerEndpoint> endpoints = new ArrayList<>();
                        JSONArray arr = json.getJSONArray("nodes");

                        for( int i=0; i<arr.length(); i++ ) {
                            LbEndpointState state = LbEndpointState.ACTIVE;
                            JSONObject item = arr.getJSONObject(i);

                            if( item.has("condition") && !item.getString("condition").equalsIgnoreCase("enabled") ) {
                                state = LbEndpointState.INACTIVE;
                            }
                            if( item.has("address") && !item.isNull("address")) {
                                String addr = item.getString("address");
                                VirtualMachine node = null;

                                for( VirtualMachine vm : getProvider().getComputeServices().getVirtualMachineSupport().listVirtualMachines() ) {
                                    RawAddress[] addrs = vm.getPublicAddresses();

                                    for( RawAddress a : addrs ){
                                        if( addr.equals(a.getIpAddress()) ) {
                                            node = vm;
                                            break;
                                        }
                                    }
                                    if( node == null ) {
                                        addrs = vm.getPrivateAddresses();
                                        for( RawAddress a : addrs ){
                                            if( addr.equals(a.getIpAddress()) ) {
                                                node = vm;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if( node != null && type.equals(LbEndpointType.VM) ) {
                                    boolean included = true;

                                    if( values.length > 0 ) {
                                        included = false;
                                        for( String value : values ) {
                                            if( value.equals(node.getProviderVirtualMachineId()) ) {
                                                included = true;
                                                break;
                                            }
                                        }
                                    }
                                    if( included ) {
                                        endpoints.add(LoadBalancerEndpoint.getInstance(LbEndpointType.VM, node.getProviderVirtualMachineId(), state));
                                    }
                                }
                                else if( node == null && type.equals(LbEndpointType.IP) ) {
                                    boolean included = true;

                                    if( values.length > 0 ) {
                                        included = false;
                                        for( String value : values ) {
                                            if( value.equals(addr) ) {
                                                included = true;
                                                break;
                                            }
                                        }
                                    }
                                    if( included ) {
                                        endpoints.add(LoadBalancerEndpoint.getInstance(LbEndpointType.IP, addr, LbEndpointState.ACTIVE));
                                    }
                                }
                            }
                        }
                        return endpoints;
                    }
                }
                return Collections.emptyList();
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to identify expected values in JSON" + e.getMessage(), e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listLoadBalancerStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listLoadBalancerStatus");
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getResource(SERVICE, RESOURCE, null, false);

            if( ob == null ) {
                return Collections.emptyList();
            }
            try {
                List<ResourceStatus> loadBalancers = new ArrayList<>();

                if( ob.has("loadBalancers") ) {
                    JSONArray lbs = ob.getJSONArray("loadBalancers");

                    if( lbs.length() > 0 ) {
                        for( int i=0; i<lbs.length(); i++ ) {
                            JSONObject tmp = lbs.getJSONObject(i);

                            if( tmp.has("id") ) {
                                JSONObject actual = method.getResource(SERVICE, RESOURCE, tmp.getString("id"), false);

                                if( actual != null && actual.has("loadBalancer") ) {
                                    ResourceStatus lb = toStatus(actual.getJSONObject("loadBalancer"));

                                    if( lb != null ) {
                                        loadBalancers.add(lb);
                                    }
                                }
                            }
                        }
                    }
                }
                return loadBalancers;
            }
            catch( JSONException e ) {
                throw new CommunicationException("Missing JSON elements for loadbalancers" + e.getMessage(), e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.testContext() != null);
    }

    @Override
    public @Nonnull Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        APITrace.begin(provider, "LB.listLoadBalancers");
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getResource(SERVICE, RESOURCE, null, false);
            
            try {
                List<LoadBalancer> loadBalancers = new ArrayList<>();
                
                if( ob.has("loadBalancers") ) {
                    JSONArray lbs = ob.getJSONArray("loadBalancers");
                    
                    if( lbs.length() > 0 ) {
                        Iterable<VirtualMachine> vms = provider.getComputeServices().getVirtualMachineSupport().listVirtualMachines();

                        for( int i=0; i<lbs.length(); i++ ) {
                            JSONObject tmp = lbs.getJSONObject(i);
                            
                            if( tmp.has("id") ) {
                                JSONObject actual = method.getResource(SERVICE, RESOURCE, tmp.getString("id"), false);
                                
                                if( actual != null && actual.has("loadBalancer") ) {
                                    LoadBalancer lb = this.toLoadBalancer(actual.getJSONObject("loadBalancer"), vms);
                                
                                    if( lb != null ) {
                                        loadBalancers.add(lb);
                                    }
                                }
                            }
                        }
                    }
                }
                return loadBalancers;
            }
            catch( JSONException e ) {
                logger.error("listLoadBalancers(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to identify expected values in JSON" + e.getMessage(), e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.removeLoadBalancer");
        try {
            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 5L);
            LoadBalancer lb;

            while( timeout > System.currentTimeMillis() ) {
                try {
                    lb = getLoadBalancer(loadBalancerId);
                    if( lb == null ) {
                        return;
                    }
                    if( !LoadBalancerState.PENDING.equals(lb.getCurrentState()) ) {
                        break;
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
            }

            NovaMethod method = new NovaMethod(provider);

            timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteResource(SERVICE, RESOURCE, loadBalancerId, null);
                    return;
                }
                catch( NovaException e ) {
                    if( e.getHttpCode() != HttpStatus.SC_CONFLICT || e.getHttpCode() == 422 ) {
                        throw e;
                    }
                }
                try { Thread.sleep(CalendarWrapper.MINUTE); }
                catch( InterruptedException ignore ) { }
            } while( System.currentTimeMillis() < timeout );
        }
        finally {
            APITrace.end();
        }
    }

    static private class Node {
        public String nodeId;
        public String address;
    }
    
    public @Nonnull Collection<Node> getNodes(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.getNodes");
        try {
            List<Node> nodes = new ArrayList<>();
            NovaMethod method = new NovaMethod(provider);
            JSONObject response = method.getResource(SERVICE, RESOURCE, loadBalancerId + "/nodes", false);

            if( response != null && response.has("nodes") ) {
                try {
                    JSONArray arr = response.getJSONArray("nodes");

                    for( int i=0; i<arr.length(); i++ ) {
                        JSONObject node = arr.getJSONObject(i);
                        Node n = new Node();

                        n.nodeId = node.getString("id");
                        n.address = node.getString("address");
                        nodes.add(n);
                    }
                }
                catch( JSONException e ) {
                    throw new CommunicationException("Unable to read nodes: " + e.getMessage(), e);
                }
            }
            return nodes;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull Collection<String> mapIPs(@Nonnull String loadBalancerId, @Nullable String[] addresses) throws CloudException, InternalException {
        TreeSet<String> nodeIds = new TreeSet<String>();

        if( addresses != null && addresses.length > 0 ) {
            Collection<Node> nodes = getNodes(loadBalancerId);

            for( String address : addresses ) {
                for( Node n : nodes ) {
                    if( n.address.equals(address) ) {
                        nodeIds.add(n.nodeId);
                        break;
                    }
                }
            }
        }
        return nodeIds;
    }

    private @Nonnull Collection<String> mapNodes(@Nonnull ProviderContext ctx, @Nonnull String loadBalancerId, @Nullable String[] serverIds) throws CloudException, InternalException {
        TreeSet<String> nodeIds = new TreeSet<String>();

        if( serverIds != null && serverIds.length > 0 ) {
            Collection<Node> nodes = getNodes(loadBalancerId);
            
            for( String serverId : serverIds ) {
                VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(serverId);
                
                if( vm != null ) {
                    boolean there = false;
                    
                    if( vm.getProviderRegionId().equals(ctx.getRegionId()) ) {
                        RawAddress[] addrs = vm.getPrivateAddresses();
                        
                        for( RawAddress addr : addrs ) {
                            for( Node n : nodes ) {
                                if( n.address.equals(addr.getIpAddress()) ) {
                                    nodeIds.add(n.nodeId);
                                    there = true;
                                    break;
                                }
                            }
                            if( there ) {
                                break;
                            }
                        }
                    }
                    if( !there ) {
                        RawAddress[] addrs = vm.getPublicAddresses();
                        
                        for( RawAddress addr : addrs ) {
                            for( Node n : nodes ) {
                                if( n.address.equals(addr.getIpAddress()) ) {
                                    nodeIds.add(n.nodeId);
                                    there = true;
                                    break;
                                }
                            }
                            if( there ) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        return nodeIds;
    }
    
    @Override
    public void removeDataCenters(@Nonnull String fromLoadBalancerId, @Nonnull String... dataCenterIdsToRemove) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No data center constraints in Rackspace");
    }

    @Override
    public void removeIPEndpoints(@Nonnull String fromLoadBalancerId, @Nonnull String ... addresses) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.removeIPEndpoints");
        try {
            LoadBalancer lb = getLoadBalancer(fromLoadBalancerId);

            if( lb == null || LoadBalancerState.TERMINATED.equals(lb.getCurrentState()) ) {
                throw new ResourceNotFoundException("No such load balancer: " + fromLoadBalancerId);
            }
            while( LoadBalancerState.PENDING.equals(lb.getCurrentState()) ) {
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
                lb = getLoadBalancer(fromLoadBalancerId);
                if( lb == null || LoadBalancerState.TERMINATED.equals(lb.getCurrentState()) ) {
                    throw new ResourceNotFoundException("No such load balancer: " + fromLoadBalancerId);
                }
            }

            Collection<String> nodeIds = mapIPs(fromLoadBalancerId, addresses);

            if( nodeIds.size() < 1 ) {
                return;
            }
            StringBuilder nodeString = new StringBuilder();

            for( String id : nodeIds ) {
                if( nodeString.length() > 0 ) {
                    nodeString.append("&");
                }
                nodeString.append("id=");
                nodeString.append(id);
            }
            NovaMethod method = new NovaMethod(provider);

            method.deleteResource(SERVICE, RESOURCE, fromLoadBalancerId + "/nodes?" + nodeString.toString(), null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeServers(@Nonnull String fromLoadBalancerId, @Nonnull String... serverIdsToRemove) throws CloudException, InternalException {
        APITrace.begin(provider, "LB.removeServers");
        try {
            LoadBalancer lb = getLoadBalancer(fromLoadBalancerId);

            if( lb == null || LoadBalancerState.TERMINATED.equals(lb.getCurrentState()) ) {
                throw new ResourceNotFoundException("No such load balancer: " + fromLoadBalancerId);
            }
            while( LoadBalancerState.PENDING.equals(lb.getCurrentState()) ) {
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
                lb = getLoadBalancer(fromLoadBalancerId);
                if( lb == null || LoadBalancerState.TERMINATED.equals(lb.getCurrentState()) ) {
                    throw new ResourceNotFoundException("No such load balancer: " + fromLoadBalancerId);
                }
            }

            Collection<String> nodeIds = mapNodes(getContext(), fromLoadBalancerId, serverIdsToRemove);

            if( nodeIds.size() < 1 ) {
                return;
            }
            StringBuilder nodeString = new StringBuilder();

            for( String id : nodeIds ) {
                if( nodeString.length() > 0 ) {
                    nodeString.append("&");
                }
                nodeString.append("id=");
                nodeString.append(id);
            }
            NovaMethod method = new NovaMethod(provider);

            method.deleteResource(SERVICE, RESOURCE, fromLoadBalancerId + "/nodes?" + nodeString.toString(), null);
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable LoadBalancer toLoadBalancer(@Nullable JSONObject json, @Nullable Iterable<VirtualMachine> possibleNodes) throws InternalException, CloudException {
        if( json == null ) {
            return null;
        }
        try {
            String dc = getContext().getRegionId() + "-a";
            String owner = getTenantId();
            String regionId = getContext().getRegionId();
            String id = (json.has("id") && !json.isNull("id")) ? json.getString("id") : null;
            String name = (json.has("name") && !json.isNull("name")) ? json.getString("name") : null;
            long created = 0L;

            if( id == null ) {
                return null;
            }
            if( regionId == null ) {
                throw new InternalException("No region was set for this request");
            }
            if( name == null ) {
                name = id;
            }
            if( json.has("created") && !json.isNull("created") ) {
                JSONObject ob = json.getJSONObject("created");

                if( ob.has("time") && !ob.isNull("time") ) {
                    created = provider.parseTimestamp(ob.getString("time"));
                }
            }
            LoadBalancerState state = LoadBalancerState.PENDING;

            if( json.has("status") && !json.isNull("status")) {
                String s = json.getString("status").toLowerCase();

                if( s.equalsIgnoreCase("active") ) {
                    state = LoadBalancerState.ACTIVE;
                }
                else if( s.equalsIgnoreCase("pending_delete") || s.equalsIgnoreCase("deleted") ) {
                    state = LoadBalancerState.TERMINATED;
                }
                else {
                    state = LoadBalancerState.PENDING;
                }
            }
            String address = null;

            if( json.has("virtualIps") ) {
                JSONArray arr = json.getJSONArray("virtualIps");

                for( int i=0; i<arr.length(); i++ ) {
                    JSONObject ob = arr.getJSONObject(i);

                    if( ob.has("ipVersion") && ob.getString("ipVersion").equalsIgnoreCase("ipv4") ) {
                        if( ob.has("address") ) {
                            address = ob.getString("address");
                            break;
                        }
                    }
                }
            }
            if( address == null ) {
                return null;
            }
            List<String> nodes = new ArrayList<>();
            int privatePort = -1;

            if( json.has("nodes") ) {
                JSONArray arr = json.getJSONArray("nodes");

                for( int i=0; i<arr.length(); i++ ) {
                    JSONObject ob = arr.getJSONObject(i);

                    if( ob.has("address") ) {
                        String addr = ob.getString("address");
                        VirtualMachine node = null;

                        if( possibleNodes != null ) {
                            for( VirtualMachine vm : possibleNodes ) {
                                RawAddress[] addrs = vm.getPublicAddresses();

                                for( RawAddress a : addrs ){
                                    if( addr.equals(a.getIpAddress()) ) {
                                        node = vm;
                                        break;
                                    }
                                }
                                if( node == null ) {
                                    addrs = vm.getPrivateAddresses();
                                    for( RawAddress a : addrs ){
                                        if( addr.equals(a.getIpAddress()) ) {
                                            node = vm;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if( node != null ) {
                            nodes.add(node.getProviderVirtualMachineId());
                        }
                    }
                    else if( ob.has("port") ) {
                        privatePort = ob.getInt("port");
                    }
                }
            }
            int port = -1;

            if( json.has("port") ) {
                port = json.getInt("port");
                if( privatePort == -1 ) {
                    privatePort = port;
                }
            }
            int[] ports = new int[] { port };

            LbProtocol protocol = LbProtocol.RAW_TCP;

            if( json.has("protocol") ) {
                String p = json.getString("protocol");

                if( p.equals("HTTP") ) {
                    protocol = LbProtocol.HTTP;
                }
                else if( p.equals("HTTPS") ) {
                    protocol = LbProtocol.HTTPS;
                }
                else if( p.equals("AJP") ) {
                    protocol = LbProtocol.AJP;
                }
            }

            LbAlgorithm algorithm = LbAlgorithm.ROUND_ROBIN;

            if( json.has("algorithm") ) {
                String a = json.getString("algorithm").toLowerCase();

                if( a.equals("round_robin") ) {
                    algorithm = LbAlgorithm.ROUND_ROBIN;
                }
                else if( a.equals("least_connections") ) {
                    algorithm = LbAlgorithm.LEAST_CONN;
                }
            }
            LoadBalancer lb = LoadBalancer.getInstance(owner, regionId, id, state, name, name + " [" + address + "]", LoadBalancerAddressType.IP, address,  ports).createdAt(created);

            if( dc != null ) {
                lb.operatingIn(dc);
            }
            lb.supportingTraffic(IPVersion.IPV4);
            lb.withListeners(LbListener.getInstance(algorithm, LbPersistence.NONE, protocol, port, privatePort));
            if( !nodes.isEmpty() ) {
                //noinspection deprecation
                lb.setProviderServerIds(nodes.toArray(new String[nodes.size()]));
            }
            return lb;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to parse response: " + e.getMessage(), e);
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject json) throws JSONException, CloudException {
        if( json == null ) {
            return null;
        }
        String id = (json.has("id") ? json.getString("id") : null);

        if( id == null || id.length() < 1 ) {
            return null;
        }
        LoadBalancerState state = LoadBalancerState.PENDING;

        if( json.has("status") ) {
            String s = json.getString("status").toLowerCase();

            if( s.equals("active") ) {
                state = LoadBalancerState.ACTIVE;
            }
            else if( s.equalsIgnoreCase("pending_delete") || s.equalsIgnoreCase("deleted") ) {
                state = LoadBalancerState.TERMINATED;
            }
            else {
                state = LoadBalancerState.PENDING;
            }
        }
        return new ResourceStatus(id, state);
    }
}
