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

package org.dasein.cloud.openstack.nova.os.network;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.AbstractVLANSupport;
import org.dasein.cloud.network.AllocationPool;
import org.dasein.cloud.network.InternetGateway;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.Networkable;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.SubnetCreateOptions;
import org.dasein.cloud.network.SubnetState;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANCapabilities;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.openstack.nova.os.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Implements Quantum network support for OpenStack clouds with Quantum networking.
 * <p>Created by George Reese: 2/15/13 11:40 PM</p>
 * @author George Reese
 * @version 2013.04 initial version
 * @since 2013.04
 */
public class Quantum extends AbstractVLANSupport<NovaOpenStack> {
    static private final Logger logger = NovaOpenStack.getLogger(Quantum.class, "std");

    private String networkVersionId = null;

    public Quantum(@Nonnull NovaOpenStack provider) {
        super(provider);
    }

    public enum QuantumType {
        NONE, RACKSPACE, NOVA, QUANTUM;

        public String getNetworkResource() {
            switch( this ) {
                case QUANTUM: return "/networks";
                case RACKSPACE: return "/os-networksv2";
                case NOVA: return "/os-networks";
            }
            return "/networks";
        }

        public String getSubnetResource() {
            switch( this ) {
                case QUANTUM: return "/subnets";
            }
            return "/subnets";
        }

        public String getPortResource() {
            switch( this ) {
                case QUANTUM: return "/ports";
            }
            return "/ports";
        }
    }

    private transient volatile NetworkCapabilities capabilities;
    @Nonnull
    @Override
    public VLANCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new NetworkCapabilities((NovaOpenStack)getProvider());
        }
        return capabilities;
    }

    public QuantumType getNetworkType() throws CloudException, InternalException {
        Cache<QuantumType> cache = Cache.getInstance(getProvider(), "quantumness", QuantumType.class, CacheLevel.CLOUD);

        Iterable<QuantumType> it = cache.get(getContext());

        if( it != null ) {
            Iterator<QuantumType> b = it.iterator();

            if( b.hasNext() ) {
                return b.next();
            }
        }
        try {
            if( ((NovaOpenStack)getProvider()).getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
                cache.put(getContext(), Collections.singletonList(QuantumType.RACKSPACE));
                return QuantumType.RACKSPACE;
            }
            try {
				JSONObject ob = getMethod().getNetworks(getNetworkResourceVersion() + QuantumType.QUANTUM.getNetworkResource(), null, false);

                if( ob != null && ob.has("networks") ) {
                    cache.put(getContext(), Collections.singletonList(QuantumType.QUANTUM));
                    return QuantumType.QUANTUM;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
            try {
				JSONObject ob = getMethod().getServers(QuantumType.NOVA.getNetworkResource(), null, false);

                if( ob != null && ob.has("networks") ) {
                    cache.put(getContext(), Collections.singletonList(QuantumType.NOVA));
                    return QuantumType.NOVA;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
            return QuantumType.NONE;
        }
        finally {
            APITrace.end();
        }
    }

    protected @Nonnull String getTenantId() throws CloudException, InternalException {
        return getContext().getAccountNumber();
    }

    public @Nonnull String createPort(@Nonnull String subnetId, @Nonnull String vmName, @Nullable String[] firewallIds) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.createPort");
        try {
            Subnet subnet = getSubnet(subnetId);

            if( subnet == null ) {
                // check is the id passed in is actually for a network
                VLAN vlan = getVlan(subnetId);
                if (vlan != null) {
                    throw new InternalException("Cannot launch into the network without a subnet");
                }
                throw new ResourceNotFoundException("subnet", subnetId);
            }
            Map<String, Object> wrapper = new HashMap<String,Object>();
            Map<String, Object> json = new HashMap<String,Object>();

            json.put("name", "Port for " + vmName);
            json.put("network_id", subnet.getProviderVlanId());
            if (firewallIds != null && firewallIds.length > 0) {
                JSONArray firewalls = new JSONArray();
                for (String firewall : firewallIds) {
                    firewalls.put(firewall);
                }
                json.put("security_groups", firewalls);
            }

            List<Map<String,Object>> ips = new ArrayList<Map<String, Object>>();
            Map<String,Object> ip = new HashMap<String, Object>();

            ip.put("subnet_id", subnetId);
            ips.add(ip);

            json.put("fixed_ips", ips);

            wrapper.put("port", json);

            JSONObject result;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                result = getMethod().postNetworks(getPortResource(), null, new JSONObject(wrapper), false);
            }
            else {
                result = getMethod().postServers(getNetworkResource() + "/" + subnet.getProviderVlanId() + "/ports", null, new JSONObject(wrapper), false);
            }
            if( result != null && result.has("port") ) {
                try {
                    JSONObject ob = result.getJSONObject("port");

                    if( ob.has("id") ) {
                        return ob.getString("id");
                    }
                }
                catch( JSONException e ) {
                    logger.error("Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Unable to parse the response", e);
                }
            }
            logger.error("No port was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No port was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    public @Nonnull Iterable<String> listPorts(@Nonnull VirtualMachine vm) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listPorts");
        try {
            JSONObject result = null;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                result = getMethod().getNetworks(getPortResource() + "?device_id=" + vm.getProviderVirtualMachineId() + "&fields=id", null, false);
            }
            else {
                result = getMethod().getServers(getNetworkResource() + "/" + vm.getProviderVlanId() + "/ports", null, false);
            }
            if( result != null && result.has("ports") ) {
                List<String> portIds = new ArrayList<String>();
                try {
                    JSONArray ports = result.getJSONArray("ports");
                    for( int i = 0; i < ports.length(); i++ ) {
                        JSONObject port = ports.getJSONObject(i);
                        if( port.has("id" ) ) {
                            portIds.add(port.getString("id"));
                        }
                    }
                } catch (JSONException e) {
                    logger.error("Unable to understand listPorts response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand listPorts response: " + e.getMessage(), e);
                }
                return portIds;
            }
            return Collections.EMPTY_LIST;
        }
        finally {
            APITrace.end();
        }
    }
    protected  @Nonnull Iterable<String> listPortsBySubnetId(@Nonnull String subnetId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listPorts");
        try {
            Subnet subnet = getSubnet(subnetId);

            JSONObject result = null;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                result = getMethod().getNetworks(getPortResource() + "?network_id=" + subnet.getProviderVlanId() + "&fields=id&fields=fixed_ips", null, false);
            }
            else {
                result = getMethod().getServers(getNetworkResource() + "/" + subnet.getProviderVlanId() + "/ports", null, false);
            }
            if( result != null && result.has("ports") ) {
                List<String> portIds = new ArrayList<String>();
                try {
                    JSONArray ports = result.getJSONArray("ports");
                    for( int i = 0; i < ports.length(); i++ ) {
                        JSONObject port = ports.getJSONObject(i);
                        boolean subnetFound = false;
                        if( port.has("fixed_ips")){
                            JSONArray ips = port.getJSONArray("fixed_ips");
                            for( int j = 0; j < ips.length(); j++ ) {
                                JSONObject fixedIp = ips.getJSONObject(j);
                                if( fixedIp.has("subnet_id") && ( subnetId.equals(fixedIp.getString("subnet_id")) ) ) {
                                    subnetFound = true;
                                    break;
                                }
                            }
                        }
                        if( port.has("id") && subnetFound ) {
                            portIds.add(port.getString("id"));
                        }
                    }
                } catch (JSONException e) {
                    logger.error("Unable to understand listPorts response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand listPorts response: " + e.getMessage(), e);
                }
                return portIds;
            }
            return Collections.EMPTY_LIST;
        }
        finally {
            APITrace.end();
        }
    }

    protected  @Nonnull Iterable<String> listPortsByNetworkId(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listPorts");
        try {
            JSONObject result = null;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                result = getMethod().getNetworks(getPortResource() + "?network_id=" + vlanId + "&fields=id", null, false);
            }
            else {
                result = getMethod().getServers(getNetworkResource() + "/" + vlanId + "/ports", null, false);
            }
            if( result != null && result.has("ports") ) {
                List<String> portIds = new ArrayList<String>();
                try {
                    JSONArray ports = result.getJSONArray("ports");
                    for( int i = 0; i < ports.length(); i++ ) {
                        JSONObject port = ports.getJSONObject(i);
                        if( port.has("id" ) ) {
                            portIds.add(port.getString("id"));
                        }
                    }
                } catch (JSONException e) {
                    logger.error("Unable to understand listPorts response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand listPorts response: " + e.getMessage(), e);
                }
                return portIds;
            }
            return Collections.EMPTY_LIST;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Subnet createSubnet(@Nonnull SubnetCreateOptions options) throws CloudException, InternalException {
        if( !getCapabilities().allowsNewSubnetCreation() ) {
            throw new OperationNotSupportedException("Subnets are not currently implemented for " + getProvider().getCloudName());
        }
        APITrace.begin(getProvider(), "VLAN.createSubnet");
        try {
            VLAN vlan = getVlan(options.getProviderVlanId());

            if( vlan == null ) {
                throw new ResourceNotFoundException("VLAN: ", options.getProviderVlanId());
            }

            Map<String,Object> wrapper = new HashMap<String,Object>();
            Map<String,Object> json = new HashMap<String,Object>();
            Map<String,Object> md = new HashMap<String, Object>();

            json.put("name", options.getName());
            json.put("cidr", options.getCidr());
            json.put("network_id", vlan.getProviderVlanId());

            IPVersion[] versions = options.getSupportedTraffic();

            if( versions.length < 1 ) {
                json.put("ip_version", "4");
            }
            else if( versions[0].equals(IPVersion.IPV6) ) {
                json.put("ip_version", "6");
            }
            else {
                json.put("ip_version", "4");
            }
            if (!getNetworkType().equals(QuantumType.QUANTUM)) {
                md.put("org.dasein.description", options.getDescription());
                json.put("metadata", md);
            }

            wrapper.put("subnet", json);

            JSONObject result = null;

            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                result = getMethod().postNetworks(getSubnetResource(), null, new JSONObject(wrapper), false);
            }
            else {
                result = getMethod().postServers(getSubnetResource(), null, new JSONObject(wrapper), false);
            }
            if( result != null && result.has("subnet") ) {
                try {
                    JSONObject ob = result.getJSONObject("subnet");
                    Subnet subnet = toSubnet(result.getJSONObject("subnet"), vlan);

                    if( subnet == null ) {
                        throw new CommunicationException("No matching subnet was generated from " + ob.toString());
                    }
                    return subnet;
                }
                catch( JSONException e ) {
                    logger.error("Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand create response: " + e.getMessage(), e);
                }
            }
            logger.error("No subnet was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No subnet was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull VLAN createVlan(@Nonnull String cidr, @Nonnull String name, @Nonnull String description, @Nonnull String domainName, @Nonnull String[] dnsServers, @Nonnull String[] ntpServers) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.createVlan");
        try {
            HashMap<String,Object> wrapper = new HashMap<String,Object>();
            HashMap<String,Object> json = new HashMap<String,Object>();
            HashMap<String,Object> md = new HashMap<String, Object>();

            if (!getNetworkType().equals(QuantumType.QUANTUM) ) {
                md.put("org.dasein.description", description);

                md.put("org.dasein.domain", domainName);
                if( dnsServers != null && dnsServers.length > 0 ) {
                    for(int i=0; i<dnsServers.length; i++ ) {
                        md.put("org.dasein.dns." + (i+1), dnsServers[i]);
                    }
                }
                if( ntpServers != null && ntpServers.length > 0 ) {
                    for(int i=0; i<ntpServers.length; i++ ) {
                        md.put("org.dasein.ntp." + (i+1), ntpServers[i]);
                    }
                }
                json.put("metadata", md);
                json.put("label", name);
                json.put("cidr", cidr);
                wrapper.put("network", json);
            }
            else {
                json.put("name", name);
                wrapper.put("network", json);
            }
            JSONObject result = null;

            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                result = getMethod().postNetworks(getNetworkResource(), null, new JSONObject(wrapper), false);
            }
            else {
                result = getMethod().postServers(getNetworkResource(), null, new JSONObject(wrapper), false);
            }
            if( result != null && result.has("network") ) {
                try {
                    JSONObject ob = result.getJSONObject("network");
                    VLAN vlan = toVLAN(result.getJSONObject("network"));
                    if( vlan == null ) {
                        throw new CommunicationException("No matching network was generated from " + ob.toString());
                    }
                    if( getNetworkType().equals(QuantumType.QUANTUM) && cidr != null ) {
                        createSubnet(SubnetCreateOptions.getInstance(vlan.getProviderVlanId(), cidr, name + "-subnet", "Auto-created subnet"));
                    }
                    return vlan;
                }
                catch( JSONException e ) {
                    logger.error("Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand create response: " + e.getMessage(), e);
                }
            }
            logger.error("No VLAN was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No VLAN was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull String getNetworkResource() throws CloudException, InternalException {
        QuantumType type = getNetworkType();
        if (type.equals(QuantumType.QUANTUM)) {
            return getNetworkResourceVersion()+QuantumType.QUANTUM.getNetworkResource();
        }
        return type.getNetworkResource();
    }

    protected  @Nonnull String getNetworkResourceVersion() throws CloudException, InternalException {
        if (networkVersionId == null) {
            try {
                JSONObject ob = getMethod().getNetworks(null, null, false);

                if( ob != null && ob.has("versions")) {
                    JSONArray versions = ob.getJSONArray("versions");
                    for (int i = 0; i<versions.length(); i++) {
                        JSONObject version = versions.getJSONObject(i);
                        if (version.has("status") && !version.isNull("status")) {
                            String status = version.getString("status");
                            if (status.equalsIgnoreCase("current")) {
                                if (version.has("id") && !version.isNull("id")) {
                                    String versionId = version.getString("id");
                                    networkVersionId = versionId;
                                }
                            }
                        }
                    }
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
        }
        return networkVersionId;
    }

    private @Nonnull String getSubnetResource() throws CloudException, InternalException {
        QuantumType type = getNetworkType();
        if (type.equals(QuantumType.QUANTUM)) {
            return getNetworkResourceVersion()+QuantumType.QUANTUM.getSubnetResource();
        }
        return type.getSubnetResource();
    }

    private @Nonnull String getPortResource() throws CloudException, InternalException {
        QuantumType type = getNetworkType();
        if (type.equals(QuantumType.QUANTUM)) {
            return getNetworkResourceVersion()+QuantumType.QUANTUM.getPortResource();
        }
        return type.getSubnetResource();
    }

    @Override
    public Subnet getSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.getSubnet");
        try {
            if( !getNetworkType().equals(QuantumType.QUANTUM) ) {
                return null;
            }
            JSONObject ob = null;

            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                ob = getMethod().getNetworks(getSubnetResource(), subnetId, false);
            }
            else {
                ob = getMethod().getServers(getSubnetResource(), subnetId, false);
            }
            try {
                if( ob != null && ob.has("subnet") ) {
                    Subnet subnet = toSubnet(ob.getJSONObject("subnet"), null);

                    if( subnet != null ) {
                        return subnet;
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for subnet in " + ob.toString(), e);
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public VLAN getVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.getVlan");
        try {
            if( vlanId.equals("00000000-0000-0000-0000-000000000000") || vlanId.equals("11111111-1111-1111-1111-111111111111") ) {
                return super.getVlan(vlanId);
            }
            JSONObject ob = null;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                ob = getMethod().getNetworks(getNetworkResource(), vlanId, false);
            }
            else {
                ob = getMethod().getServers(getNetworkResource(), vlanId, false);
            }
            try {
                if( ob != null && ob.has("network") ) {
                    VLAN v = toVLAN(ob.getJSONObject("network"));

                    if( v != null ) {
                        return v;
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for networks in " + ob.toString(), e);
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.isSubscribed");
        try {
            JSONObject ob = null;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                ob = getMethod().getNetworks(getNetworkResource(), null, false);
            }
            else {
                ob = getMethod().getServers(getNetworkResource(), null, false);
            }
            return (ob != null && ob.has("networks"));
        }
        finally {
            APITrace.end();
        }
    }

    protected @Nonnull ComputeServices getServices() throws CloudException, InternalException {
        return getProvider().getComputeServices();
    }

    @Override
    public @Nonnull Iterable<Networkable> listResources(@Nonnull String inVlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listResources");
        try {
            ArrayList<Networkable> list = new ArrayList<Networkable>();

            if( getServices() != null ) {
                VirtualMachineSupport vmSupport = getServices().getVirtualMachineSupport();

                if( vmSupport != null ) {
                    for( VirtualMachine vm : vmSupport.listVirtualMachines() ) {
                        if( inVlanId.equals(vm.getProviderVlanId()) ) {
                            list.add(vm);
                        }
                    }
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<Subnet> listSubnets(@Nonnull String inVlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listSubnets");
        try {
            if( !getNetworkType().equals(QuantumType.QUANTUM) ) {
                return Collections.emptyList();
            }
            JSONObject ob;

            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                ob = getMethod().getNetworks(getSubnetResource(), null, false);
            }
            else {
                ob = getMethod().getServers(getSubnetResource(), null, false);
            }
            ArrayList<Subnet> subnets = new ArrayList<Subnet>();

            try {
                if( ob != null && ob.has("subnets") ) {
                    JSONArray list = ob.getJSONArray("subnets");

                    for( int i=0; i<list.length(); i++ ) {
                        Subnet subnet = toSubnet(list.getJSONObject(i), null);

                        if( subnet != null && subnet.getProviderVlanId().equals(inVlanId) ) {
                            subnets.add(subnet);
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for subnets in " + ob.toString(), e);
            }
            return subnets;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVlanStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listVlanStatus");
        try {
            JSONObject ob;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                ob = getMethod().getNetworks(getNetworkResource(), null, false);
            }
            else {
                ob = getMethod().getServers(getNetworkResource(), null, false);
            }
            ArrayList<ResourceStatus> networks = new ArrayList<ResourceStatus>();

            try {
                if( ob != null && ob.has("networks") ) {
                    JSONArray list = ob.getJSONArray("networks");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject net = list.getJSONObject(i);
                        ResourceStatus status = toStatus(net);

                        if( status != null ) {
                            if( status.getProviderResourceId().equals("00000000-0000-0000-0000-000000000000") || status.getProviderResourceId().equals("11111111-1111-1111-1111-111111111111") ) {
                                continue;
                            }
                            networks.add(status);
                        }

                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for networks in " + ob.toString(), e);
            }
            return networks;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listVlans");
        try {
            JSONObject ob = null;
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                ob = getMethod().getNetworks(getNetworkResource(), null, false);
            }
            else {
                ob = getMethod().getServers(getNetworkResource(), null, false);
            }
            ArrayList<VLAN> networks = new ArrayList<VLAN>();

            try {
                if( ob != null && ob.has("networks") ) {
                    JSONArray list = ob.getJSONArray("networks");

                    for( int i=0; i<list.length(); i++ ) {
                        VLAN v = toVLAN(list.getJSONObject(i));

                        if( v != null ) {
                            if( v.getProviderVlanId().equals("00000000-0000-0000-0000-000000000000") || v.getProviderVlanId().equals("11111111-1111-1111-1111-111111111111") ) {
                                continue;
                            }
                            networks.add(v);
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for networks in " + ob.toString(), e);
            }
            return networks;
        }
        finally {
            APITrace.end();
        }
    }

    protected NovaMethod getMethod() {
        return new NovaMethod(getProvider());
    }

    @Override
    public void removeInternetGatewayById(@Nonnull String id) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removePort(@Nonnull String portId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.removePort");
        try {
            if( !getNetworkType().equals(QuantumType.QUANTUM) ) {
                throw new OperationNotSupportedException("Cannot remove port in an OpenStack network of type: " + getNetworkType());
            }
            getMethod().deleteNetworks(getPortResource(), portId + ".json");
        }
        catch( CloudException e ) {
            if( e.getHttpCode() == HttpStatus.SC_NOT_FOUND ) {
                logger.warn("Error while deleting port ["+portId+"], but it is probably fine");
            }
            else {
                throw e;
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeSubnet(String subnetId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.removeSubnet");
        try {
            if( !getNetworkType().equals(QuantumType.QUANTUM) ) {
                throw new OperationNotSupportedException("Cannot remove subnets in an OpenStack network of type: " + getNetworkType());
            }

            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                Iterable<String> portIds = listPortsBySubnetId(subnetId);
                for (String portId : portIds) {
                    removePort(portId);
                }
                getMethod().deleteNetworks(getSubnetResource(), subnetId);
            }
            else {
                getMethod().deleteServers(getSubnetResource(), subnetId);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.removeVlan");
        try {
            if (getNetworkType().equals(QuantumType.QUANTUM) ) {
                Iterable<String> portIds = listPortsByNetworkId(vlanId);
                for (String portId : portIds) {
                    removePort(portId);
                }

                getMethod().deleteNetworks(getNetworkResource(), vlanId);
            }
            else {
                getMethod().deleteServers(getNetworkResource(), vlanId);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void updateInternetGatewayTags(@Nonnull String internetGatewayId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    protected @Nonnull VLANState toState(@Nonnull String s) {
        if( s.equalsIgnoreCase("active") ) {
            return VLANState.AVAILABLE;
        }
        else if( s.equalsIgnoreCase("build") ) {
            return VLANState.PENDING;
        }
        return VLANState.PENDING;
    }

    protected @Nullable ResourceStatus toStatus(@Nonnull JSONObject network) throws CloudException, InternalException {
        try {
            String id = (network.has("id") ? network.getString("id") : null);

            if( id == null ) {
                return null;
            }
            VLANState s = (network.has("status") ? toState(network.getString("status")) : VLANState.AVAILABLE);

            return new ResourceStatus(id, s);
        }
        catch( JSONException e ) {
            throw new CommunicationException("Invalid JSON from cloud: " + e.getMessage(), e);
        }
    }

    protected @Nullable Subnet toSubnet(@Nonnull JSONObject json, @Nullable VLAN vlan) throws CloudException, InternalException {
        try {
            if( vlan == null ) {
                String vlanId = (json.has("network_id") ? json.getString("network_id") : null);

                if( vlanId == null ) {
                    return null;
                }
                vlan = getVlan(vlanId);
                if( vlan == null ) {
                    return null;
                }
            }
            String subnetId;

            if( json.has("id") ) {
                subnetId = json.getString("id");
            }
            else {
                return null;
            }

            String cidr = (json.has("cidr") ? json.getString("cidr") : vlan.getCidr());
            String name = (json.has("name") ? json.getString("name") : null);
            String description = (json.has("description") ? json.getString("description") : null);

            Map<String,String> metadata = new HashMap<String, String>();

            if( json.has("metadata") ) {
                JSONObject md = json.getJSONObject("metadata");
                String[] names = JSONObject.getNames(md);

                if( names != null && names.length > 0 ) {
                    for( String n : names ) {
                        String value = md.getString(n);

                        if( value != null ) {
                            metadata.put(n, value);
                            if( n.equals("org.dasein.description") && description == null ) {
                                description = value;
                            }
                            else if( n.equals("org.dasein.name") && name == null ) {
                                name = value;
                            }
                        }
                    }
                }
            }
            if( name == null ) {
                name = subnetId + " - " + cidr;
            }
            if( description == null ) {
                description = name;
            }
            IPVersion traffic = IPVersion.IPV4;

            if( json.has("ip_version") ) {
                String version = json.getString("ip_version");

                if( version.equals("6") ) {
                    traffic = IPVersion.IPV6;
                }
            }
            Subnet subnet = Subnet.getInstance(vlan.getProviderOwnerId(), vlan.getProviderRegionId(), vlan.getProviderVlanId(), subnetId, SubnetState.AVAILABLE, name, description, cidr).supportingTraffic(traffic);
            // FIXME: REMOVE: subnets are not constrained to a dc in openstack
            //            Iterable<DataCenter> dc = getProvider().getDataCenterServices().listDataCenters(vlan.getProviderRegionId());
            //            subnet.constrainedToDataCenter(dc.iterator().next().getProviderDataCenterId());

            if( json.has("allocation_pools") ) {
                JSONArray p = json.getJSONArray("allocation_pools");

                if( p.length() > 0 ) {
                    AllocationPool[] pools = new AllocationPool[p.length()];

                    for( int i=0; i<p.length(); i++ ) {
                        JSONObject ob = p.getJSONObject(i);
                        String start = null, end = null;

                        if( ob.has("start") ) {
                            start = ob.getString("start");
                        }
                        if( ob.has("end") ) {
                            end = ob.getString("end");
                        }
                        if( start == null ) {
                            start = end;
                        }
                        else if( end == null ) {
                            end = start;
                        }
                        if( start != null ) {
                            pools[i] = AllocationPool.getInstance(new RawAddress(start), new RawAddress(end));
                        }
                    }
                    // begin hocus pocus to deal with the external possibility of a bad allocation pool
                    int count = 0;

                    for( AllocationPool pool : pools ) {
                        if( pool != null ) {
                            count++;
                        }
                    }
                    if( count != pools.length ) {
                        ArrayList<AllocationPool> list = new ArrayList<AllocationPool>();

                        for( AllocationPool pool : pools ) {
                            if( pool != null ) {
                                list.add(pool);
                            }
                        }
                        pools = list.toArray(new AllocationPool[list.size()]);
                    }
                    // end hocus pocus
                    subnet.havingAllocationPools(pools);
                }
            }
            if( json.has("gateway_ip") ) {
                subnet.usingGateway(new RawAddress(json.getString("gateway_ip")));
            }
            if( !metadata.isEmpty() ) {
                for( Map.Entry<String,String> entry : metadata.entrySet() ) {
                    subnet.setTag(entry.getKey(), entry.getValue());
                }
            }
            return subnet;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Invalid JSON from cloud: " + e.getMessage(), e);
        }
    }

    protected String getCurrentRegionId() throws InternalException {
        return getContext().getRegionId();
    }
    protected @Nullable VLAN toVLAN(@Nonnull JSONObject network) throws CloudException, InternalException {
        try {
            VLAN v = new VLAN();

            v.setProviderOwnerId(getTenantId());
            v.setCurrentState(VLANState.AVAILABLE);
            v.setProviderRegionId(getCurrentRegionId());
            // FIXME: REMOVE: vlans are not constrained to a DC in openstack
            //            Iterable<DataCenter> dc = getProvider().getDataCenterServices().listDataCenters(getContext().getRegionId());
            //            v.setProviderDataCenterId(dc.iterator().next().getProviderDataCenterId());
            v.setVisibleScope(VisibleScope.ACCOUNT_REGION);

            if( network.has("id") ) {
                v.setProviderVlanId(network.getString("id"));
            }
            if( network.has("name") ) {
                v.setName(network.getString("name"));
            }
            else if( network.has("label") ) {
                v.setName(network.getString("label"));
            }
            if( network.has("cidr") ) {
                v.setCidr(network.getString("cidr"));
            }
            if( network.has("status") ) {
                v.setCurrentState(toState(network.getString("status")));
            }
            if( network.has("metadata") ) {
                JSONObject md = network.getJSONObject("metadata");
                String[] names = JSONObject.getNames(md);

                if( names != null && names.length > 0 ) {
                    for( String n : names ) {
                        String value = md.getString(n);

                        if( value != null ) {
                            v.setTag(n, value);
                            if( n.equals("org.dasein.description") && v.getDescription() == null ) {
                                v.setDescription(value);
                            }
                            else if( n.equals("org.dasein.domain") && v.getDomainName() == null ) {
                                v.setDomainName(value);
                            }
                            else if( n.startsWith("org.dasein.dns.") && !n.equals("org.dasein.dsn.") && v.getDnsServers().length < 1 ) {
                                List<String> dns = new ArrayList<String>();

                                try {
                                    int idx = Integer.parseInt(n.substring("org.dasein.dns.".length() + 1));
                                    if( value != null ) {
                                        dns.add(idx - 1, value);
                                    }
                                }
                                catch( NumberFormatException ignore ) {
                                    // ignore
                                }
                                v.setDnsServers(dns.toArray(new String[dns.size()]));
                            }
                            else if( n.startsWith("org.dasein.ntp.") && !n.equals("org.dasein.ntp.") && v.getNtpServers().length < 1 ) {
                                List<String> ntp = new ArrayList<String>();

                                try {
                                    int idx = Integer.parseInt(n.substring("org.dasein.ntp.".length()));
                                    if( value != null ) {
                                        ntp.add(idx - 1, value);
                                    }
                                }
                                catch( NumberFormatException ignore ) {
                                }
                                v.setNtpServers(ntp.toArray(new String[ntp.size()]));
                            }
                        }
                    }
                }
            }
            if( v.getProviderVlanId() == null ) {
                return null;
            }
            if( v.getCidr() == null ) {
                v.setCidr("0.0.0.0/0");
            }
            if( v.getName() == null ) {
                v.setName(v.getCidr());
                if( v.getName() == null ) {
                    v.setName(v.getProviderVlanId());
                }
            }
            if( v.getDescription() == null ) {
                v.setDescription(v.getName());
            }
            v.setSupportedTraffic(IPVersion.IPV4, IPVersion.IPV6);
            return v;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Invalid JSON from cloud: " + e.getMessage(), e);
        }
    }
}
