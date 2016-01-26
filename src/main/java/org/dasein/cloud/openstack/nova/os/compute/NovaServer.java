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

package org.dasein.cloud.openstack.nova.os.compute;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANSupport;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.OpenStackProvider;
import org.dasein.cloud.openstack.nova.os.network.NovaFloatingIP;
import org.dasein.cloud.openstack.nova.os.network.NovaNetworkServices;
import org.dasein.cloud.openstack.nova.os.network.NovaSecurityGroup;
import org.dasein.cloud.openstack.nova.os.network.Quantum;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Implements services supporting interaction with cloud virtual machines.
 * @author George Reese (george.reese@imaginary.com)
 * @version 2012.09 addressed issue with alternate security group lookup in some OpenStack environments (issue #1)
 * @version 2013.02 implemented setting kernel/ramdisk image IDs (see issue #40 in dasein-cloud-core)
 * @version 2013.02 updated with support for Dasein Cloud 2013.02 model
 * @version 2013.02 added support for fetching shell keys (issue #4)
 * @since unknown
 */
public class NovaServer extends AbstractVMSupport<NovaOpenStack> {
    static private final Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

    static public final String SERVICE = "compute";
    public static final String ORG_DASEIN_PORT_ID = "org.dasein.portId";

    NovaServer(NovaOpenStack provider) {
        super(provider);
    }

    @Nonnull protected String getTenantId() throws CloudException, InternalException {
        return getContext().getAccountNumber();
    }

    private transient volatile NovaServerCapabilities capabilities;

    @Nonnull @Override
    public VirtualMachineCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new NovaServerCapabilities(getProvider());
        }
        return capabilities;
    }

    protected NovaMethod getMethod() {
        return new NovaMethod(getProvider());
    }

    protected Quantum getQuantum() {
        if( getNetworkServices() == null ) {
            return null;
        }
        else {
            return getNetworkServices().getVlanSupport();
        }
    }

    protected NovaFloatingIP getNovaFloatingIp() {
        if( getNetworkServices() == null ) {
            return null;
        }
        else {
            return getNetworkServices().getIpAddressSupport();
        }
    }

    protected NovaSecurityGroup getNovaSecurityGroup() {
        if( getNetworkServices() == null ) {
            return null;
        }
        else {
            return getNetworkServices().getFirewallSupport();
        }
    }

    protected int getMinorVersion() throws CloudException, InternalException {
        return getProvider().getMinorVersion();
    }

    protected int getMajorVersion() throws CloudException, InternalException {
        return getProvider().getMajorVersion();
    }

    protected String getRegionId() throws InternalException {
        return getContext().getRegionId();
    }

    protected Platform getPlatform(String vmName, String vmDescription, String imageId) throws CloudException, InternalException {
        Platform p = Platform.guess(vmName + " " + vmDescription);

        if( p.equals(Platform.UNKNOWN) ) {
            if( imageId != null ) {
                MachineImage img = getProvider().getComputeServices().getImageSupport().getImage(imageId);
                if( img != null ) {
                    p = img.getPlatform();
                }
            }
        }

        return p;
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.getConsoleOutput");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new ResourceNotFoundException("No such virtual machine: " + vmId);
            }
            Map<String, Object> json = new HashMap<>();

            json.put("os-getConsoleOutput", new HashMap<String, Object>());

            String console = getMethod().postServersForString("/servers", vmId, new JSONObject(json), true);

            return ( console == null ? "" : console );
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.getProduct");
        try {
            for( FlavorRef flavor : listFlavors() ) {
                if( flavor.product.getProviderProductId().equals(productId) ) {
                    return flavor.product;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    protected NovaNetworkServices getNetworkServices() {
        return getProvider().getNetworkServices();
    }

    protected OpenStackProvider getCloudProvider() {
        return getProvider().getCloudProvider();
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.getVirtualMachine");
        try {
            JSONObject ob = getMethod().getServers("/servers", vmId, true);

            if( ob == null ) {
                return null;
            }
            Iterable<IpAddress> ipv4, ipv6;
            Iterable<VLAN> networks;

            NetworkServices services = getNetworkServices();
            if( services != null ) {
                IpAddressSupport support = services.getIpAddressSupport();

                if( support != null ) {
                    ipv4 = support.listIpPool(IPVersion.IPV4, false);
                    ipv6 = support.listIpPool(IPVersion.IPV6, false);
                }
                else {
                    ipv4 = ipv6 = Collections.emptyList();
                }

                VLANSupport vs = services.getVlanSupport();

                if( vs != null ) {
                    networks = vs.listVlans();
                }
                else {
                    networks = Collections.emptyList();
                }
            }
            else {
                ipv4 = ipv6 = Collections.emptyList();
                networks = Collections.emptyList();
            }
            try {
                if( ob.has("server") ) {
                    JSONObject server = ob.getJSONObject("server");
                    VirtualMachine vm = toVirtualMachine(server, ipv4, ipv6, networks);

                    if( vm != null ) {
                        return vm;
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("getVirtualMachine(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for servers", e);
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    protected @Nonnull String getServerStatus(@Nonnull String virtualMachineId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.getVmStatus");
        try {
            final JSONObject ob = getMethod().getServers("/servers", virtualMachineId, true);
            return ob.getJSONObject("server").getString("status");
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to parse the server status response", e);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull VirtualMachine alterVirtualMachineProduct(@Nonnull String virtualMachineId, @Nonnull String productId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.resize");
        try {
            Map<String, Object> json = new HashMap<>();
            Map<String, Object> action = new HashMap<>();

            action.put("flavorRef", productId);
            json.put("resize", action);

            getMethod().postServers("/servers", virtualMachineId, new JSONObject(json), true);
            String status;
            while( "resize".equalsIgnoreCase(status = getServerStatus(virtualMachineId)) ) {
                try {
                    Thread.sleep(5000L);
                }
                catch( InterruptedException e ) {
                }
            }
            if( "verify_resize".equalsIgnoreCase(status) ) {
                json.clear();
                json.put("confirmResize", null);
                getMethod().postServers("/servers", virtualMachineId, new JSONObject(json), true);
            }
            VirtualMachine vm = getVirtualMachine(virtualMachineId);
            if( status.equals("ACTIVE") && !( vm.getProductId().equals(productId) ) ) {
                throw new GeneralCloudException("Failed to resize VM from " + getProduct(vm.getProductId()).getName() + " to " + getProduct(productId).getName(),
                        CloudErrorType.GENERAL);
            }
            return vm;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.isSubscribed");
        try {
            return ( getProvider().testContext() != null );
        }
        finally {
            APITrace.end();
        }
    }

    protected MachineImage getImage(String providerImageId) throws CloudException, InternalException {
        return getProvider().getComputeServices().getImageSupport().getImage(providerImageId);
    }

    protected void prepareFirewallsForLaunch(VMLaunchOptions options, Map<String, Object> json) throws CloudException, InternalException {
        if( options.getFirewallIds().length == 0 ) {
            return;
        }
        List<Map<String,Object>> firewalls = new ArrayList<>();

        FirewallSupport support = getNovaSecurityGroup();
        if( support != null ) {
            for( String id : options.getFirewallIds() ) {
                Firewall firewall = support.getFirewall(id);
                if( firewall != null ) {
                    Map<String,Object> fw = new HashMap<>();

                    fw.put("name", firewall.getName());
                    firewalls.add(fw);
                }
            }
        }
    }

    protected void prepareVlanForLaunch(VMLaunchOptions options, Map<String, Object> json) throws CloudException, InternalException {
        List<Map<String,Object>> vlans = new ArrayList<>();
        Map<String,Object> vlan = new HashMap<>();

        if( options.getVlanId() != null ) {
            vlan.put("uuid", options.getVlanId());
            vlans.add(vlan);
            json.put("networks", vlans);
        }
        else {
            if( options.getSubnetId() != null && !getProvider().isRackspace() ) {
                Quantum support = getQuantum();

                if( support != null ) {
                    try {
                        String portId = support.createPort(options.getSubnetId(), options.getHostName(), options.getFirewallIds());
                        vlan.put("port", portId);
                        vlans.add(vlan);
                        json.put("networks", vlans);
                        options.withMetaData(ORG_DASEIN_PORT_ID, portId);
                    }
                    catch (CloudException e) {
                        if (e.getHttpCode() != 403) {
                            throw e;
                        }

                        logger.warn("Unable to create port - trying to launch into general network");
                        Subnet subnet = support.getSubnet(options.getSubnetId());

                        vlan.put("uuid", subnet.getProviderVlanId());
                        vlans.add(vlan);
                        json.put("networks", vlans);
                    }
                }
            }
        }
    }
    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.launch");
        VirtualMachine vm = null;

        try {
            MachineImage targetImage = getImage(options.getMachineImageId());
            if( targetImage == null ) {
                throw new ResourceNotFoundException("No such machine image: " + options.getMachineImageId());
            }
            //Additional LPAR Call
            boolean isBareMetal = isBareMetal(options.getMachineImageId());

            Map<String,Object> wrapper = new HashMap<>();
            Map<String,Object> json = new HashMap<>();

            json.put("name", options.getHostName());
            if( options.getBootstrapPassword() != null ) {
                json.put("adminPass", options.getBootstrapPassword());
            }
            if( options.getUserData() != null ) {
                try {
                    json.put("user_data", Base64.encodeBase64String(options.getUserData().getBytes("utf-8")));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
            }
            if( getMinorVersion() == 0 && getMajorVersion() == 1 ) {
                json.put("imageId", String.valueOf(options.getMachineImageId()));
                json.put("flavorId", options.getStandardProductId());
            }
            else {
                if( getCloudProvider().equals(OpenStackProvider.HP) ) {
                    json.put("imageRef", options.getMachineImageId());
                }
                else {
                    json.put("imageRef", getImageRef(options));
                }
                json.put("flavorRef", getFlavorRef(options.getStandardProductId()));
            }

            prepareVlanForLaunch(options, json);
            prepareFirewallsForLaunch(options, json);

            if( options.getBootstrapKey() != null ) {
                json.put("key_name", options.getBootstrapKey());
            }

            if( isBareMetal ) {
                Map<String, String> blockDeviceMapping = new HashMap<String, String>();
                //blockDeviceMapping.put("device_name", "/dev/sdb1");
                blockDeviceMapping.put("boot_index", "0");
                blockDeviceMapping.put("uuid", getImageRef(options));
                //blockDeviceMapping.put("guest_format", "ephemeral");
                String volumeSize = "";
                if( targetImage.getTag("minDisk") != null ) {
                    volumeSize = (String)targetImage.getTag("minDisk");
                }
                else {
                    String minSize = (String)targetImage.getTag("minSize");
                    volumeSize = roundUpToGB(Long.valueOf(minSize)) + "";
                }
                blockDeviceMapping.put("volume_size", volumeSize);
                blockDeviceMapping.put("source_type", "image");
                blockDeviceMapping.put("destination_type", "volume");
                blockDeviceMapping.put("delete_on_termination", "True");
                json.put("block_device_mapping_v2", blockDeviceMapping);
            }

            if( !targetImage.getPlatform().equals(Platform.UNKNOWN) ) {
                options.withMetaData("org.dasein.platform", targetImage.getPlatform().name());
            }
            options.withMetaData("org.dasein.description", options.getDescription());
            Map<String, Object> tmpMeta = options.getMetaData();
            Map<String, Object> newMeta = new HashMap<>();
            for (Map.Entry entry : tmpMeta.entrySet()) {
                if (entry.getValue() != null) { //null values not supported by openstack
                    newMeta.put(entry.getKey().toString(), entry.getValue());
                }
            }
            json.put("metadata", newMeta);
            wrapper.put("server", new JSONObject(json));
            JSONObject result = getMethod().postServers(isBareMetal ? "/os-volumes_boot" : "/servers", null, new JSONObject(wrapper), true);

            if( result.has("server") ) {
                try {
                    Collection<IpAddress> ips = Collections.emptyList();
                    Collection<VLAN> nets = Collections.emptyList();

                    JSONObject server = result.getJSONObject("server");
                    vm = toVirtualMachine(server, ips, ips, nets);

                    if( vm != null ) {
                        String vmId = vm.getProviderVirtualMachineId();
                        long timeout = System.currentTimeMillis() + 5 * 60 * 1000;
                        while(( vm == null || vm.getCurrentState() == null ) && System.currentTimeMillis() < timeout ) {
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException ignore) {}
                            vm = getVirtualMachine(vmId);
                        }
                        if( vm == null || vm.getCurrentState() == null ) {
                            throw new GeneralCloudException("VM failed to launch with a meaningful status", CloudErrorType.GENERAL);
                        }
                        return vm;
                    }
                }
                catch( JSONException e ) {
                    logger.error("launch(): Unable to understand launch response: " + e.getMessage());
                    throw new CommunicationException("Invalid response", e);
                }
            }
            logger.error("launch(): No server was created by the launch attempt, and no error was returned");
            throw new GeneralCloudException("No virtual machine was launched", CloudErrorType.GENERAL);

        }
        finally {
            cleanupFailedLaunch(options, vm);
            APITrace.end();
        }
    }

    protected void cleanupFailedLaunch(VMLaunchOptions options, VirtualMachine vm) throws CloudException, InternalException {
        //if launch fails or instance in error state - remove port
        if( options.getMetaData().containsKey(ORG_DASEIN_PORT_ID) && (vm == null || VmState.ERROR.equals(vm.getCurrentState())) ) {
            Quantum quantum = getProvider().getNetworkServices().getVlanSupport();
            if( quantum != null ) {
                quantum.removePort(( String ) options.getMetaData().get(ORG_DASEIN_PORT_ID));
            }
        }
    }

    protected boolean isBareMetal(String machineImageId) {
        try{
            String lparMetadataKey = "hypervisor_type";
            String lparMetadataValue = "Hitachi";
            JSONObject ob = getMethod().getServers("/images/" + machineImageId + "/metadata", lparMetadataKey, false);
            if(ob.has("metadata")){
                JSONObject metadata = ob.getJSONObject("metadata");
                if(metadata.has(lparMetadataKey) && metadata.getString(lparMetadataKey).equals(lparMetadataValue)) {
                    return true;
                }
            }
        }
        catch(Exception ex){
            //Something failed while checking Hitachi LPAR metadata
            logger.trace("Failed to find Hitachi LPAR metadata");
        }
        return false;
    }

    protected String getImageRef(VMLaunchOptions options) throws CloudException, InternalException {
        return getProvider().getComputeServices().getImageSupport().getImageRef(options.getMachineImageId());
    }

    public static int roundUpToGB(Long size) {
        Double round = Math.ceil(size / Math.pow(2, 30));
        return round.intValue();
    }

    protected  @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId, @Nonnull JSONObject server) throws InternalException, CloudException {
        try {
            if( server.has("security_groups") ) {

                FirewallSupport support = getNovaSecurityGroup();
                Iterable<Firewall> firewalls = null;
                if( support != null ) {
                    firewalls = support.list();
                }
                if( firewalls == null ) {
                    firewalls = Collections.emptyList();
                }
                JSONArray groups = server.getJSONArray("security_groups");
                List<String> results = new ArrayList<>();

                for( int i=0; i<groups.length(); i++ ) {
                    JSONObject group = groups.getJSONObject(i);
                    String id = group.has("id") ? group.getString("id") : null;
                    String name = group.has("name") ? group.getString("name") : null;

                    if( id != null || name != null  ) {
                        for( Firewall fw : firewalls ) {
                            if( id != null ) {
                                if( id.equals(fw.getProviderFirewallId()) ) {
                                    results.add(id);
                                }
                            }
                            else if( name.equals(fw.getName()) ) {
                                results.add(fw.getProviderFirewallId());
                            }
                        }
                    }
                }
                return results;
            }
            else {
                List<String> results = new ArrayList<>();

                JSONObject ob = getMethod().getServers("/os-security-groups/servers", vmId + "/os-security-groups", true);

                if( ob != null ) {

                    if( ob.has("security_groups") ) {
                        JSONArray groups = ob.getJSONArray("security_groups");

                        for( int i=0; i<groups.length(); i++ ) {
                            JSONObject group = groups.getJSONObject(i);

                            if( group.has("id") ) {
                                results.add(group.getString("id"));
                            }
                        }
                    }
                }
                return results;
            }
        }
        catch( JSONException e ) {
            logger.error("Unable to understand listFirewalls response: " + e.getMessage());
            throw new CommunicationException("Unable to understand listFirewalls response: " + e.getMessage(), e);        }
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listFirewalls");
        try {
            JSONObject ob = getMethod().getServers("/servers", vmId, true);

            if( ob == null ) {
                return Collections.emptyList();
            }
            try {
                if( ob.has("server") ) {
                    JSONObject server = ob.getJSONObject("server");

                    return listFirewalls(vmId, server);
                }
                throw new ResourceNotFoundException("No such server found for " + vmId);
            }
            catch( JSONException e ) {
                logger.error("listFirewalls(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to understand listFirewalls response: " + e.getMessage(), e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    static public class FlavorRef {
        public String id;
        public String[][] links;
        VirtualMachineProduct product;

        public String toString() { return (id + " -> " + product); }
    }

    @Nullable protected Iterable<FlavorRef> listCachedFlavors() throws InternalException {
        Cache<FlavorRef> cache = Cache.getInstance(getProvider(), "flavorRefs", FlavorRef.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
        Iterable<FlavorRef> refs = cache.get(getContext());
        return refs;
    }

    protected void cacheFlavors(Iterable<FlavorRef> refs) throws InternalException {
        Cache<FlavorRef> cache = Cache.getInstance(getProvider(), "flavorRefs", FlavorRef.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
        cache.put(getContext(), refs);
    }


    @Nonnull protected Iterable<FlavorRef> listFlavors() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listFlavors");
        try {
            Iterable<FlavorRef> cachedRefs = listCachedFlavors();
            if( cachedRefs != null ) {
                return cachedRefs;
            }

            JSONObject ob = getMethod().getServers("/flavors", null, true);
            List<FlavorRef> flavors = new ArrayList<>();

            try {
                if( ob != null && ob.has("flavors") ) {
                    JSONArray list = ob.getJSONArray("flavors");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject p = list.getJSONObject(i);
                        FlavorRef ref = new FlavorRef();

                        if( p.has("id") ) {
                            ref.id = p.getString("id");
                        }
                        else {
                            continue;
                        }
                        if( p.has("links") ) {
                            JSONArray links = p.getJSONArray("links");

                            ref.links = new String[links.length()][];
                            for( int j=0; j<links.length(); j++ ) {
                                JSONObject link = links.getJSONObject(j);

                                ref.links[j] = new String[2];
                                if( link.has("rel") ) {
                                    ref.links[j][0] = link.getString("rel");
                                }
                                if( link.has("href") ) {
                                    ref.links[j][1] = link.getString("href");
                                }
                            }
                        }
                        else {
                            ref.links = new String[0][];
                        }
                        ref.product = toProduct(p);
                        if( ref.product != null ) {
                            flavors.add(ref);
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("listProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to understand listProducts response: " + e.getMessage(), e);
            }
            cacheFlavors(flavors);
            return flavors;
        }
        finally {
            APITrace.end();
        }
    }

    public @Nullable String getFlavorRef(@Nonnull String flavorId) throws InternalException, CloudException {
        for( FlavorRef ref : listFlavors() ) {
            if( ref.id.equals(flavorId) ) {
                String def = null;

                for( String[] link : ref.links ) {
                    if( link[0] != null && link[0].equals("self") && link[1] != null ) {
                        return link[1];
                    }
                    else if( def == null && link[1] != null ) {
                        def = link[1];
                    }
                }
                return def;
            }
        }
        return null;
    }

    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listAllProducts() throws CloudException, InternalException{
        return listProducts(null, VirtualMachineProductFilterOptions.getInstance());
    }

    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listProducts(@Nonnull String machineImageId, @Nullable VirtualMachineProductFilterOptions options) throws InternalException, CloudException {

        APITrace.begin(getProvider(), "VM.listProducts");
        try {
            List<VirtualMachineProduct> products = new ArrayList<>();

            for( FlavorRef flavor : listFlavors() ) {
                if (options != null) {
                    if (options.matches(flavor.product)) {
                        products.add(flavor.product);
                    }
                }
                else {
                    products.add(flavor.product);
                }
            }
            return products;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listVirtualMachineStatus");
        try {
            JSONObject ob = getMethod().getServers("/servers", null, true);
            List<ResourceStatus> servers = new ArrayList<>();

            try {
                if( ob != null && ob.has("servers") ) {
                    JSONArray list = ob.getJSONArray("servers");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject server = list.getJSONObject(i);
                        ResourceStatus vm = toStatus(server);

                        if( vm != null ) {
                            servers.add(vm);
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("listVirtualMachines(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to understand listVirtualMachines response: " + e.getMessage(), e);
            }
            return servers;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listVirtualMachines");
        try {
            JSONObject ob = getMethod().getServers("/servers", null, true);
            List<VirtualMachine> servers = new ArrayList<>();

            Iterable<IpAddress> ipv4 = Collections.emptyList(), ipv6 = Collections.emptyList();
            Iterable<VLAN> nets = Collections.emptyList();

            IpAddressSupport support = getNovaFloatingIp();
            if( support != null ) {
                ipv4 = support.listIpPool(IPVersion.IPV4, false);
                ipv6 = support.listIpPool(IPVersion.IPV6, false);
            }

            VLANSupport vs = getQuantum();
            if( vs != null ) {
                nets = vs.listVlans();
            }

            try {
                if( ob != null && ob.has("servers") ) {
                    JSONArray list = ob.getJSONArray("servers");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject server = list.getJSONObject(i);
                        VirtualMachine vm = toVirtualMachine(server, ipv4, ipv6, nets);

                        if( vm != null ) {
                            servers.add(vm);
                        }

                    }
                }
            }
            catch( JSONException e ) {
                logger.error("listVirtualMachines(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to understand listVirtualMachines response: " + e.getMessage(), e);
            }
            return servers;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void pause(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.pause");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new ResourceNotFoundException("No such virtual machine found for " + vmId);
            }
            if( !getCapabilities().supportsPause() ) {
                throw new OperationNotSupportedException("Pause/unpause is not supported in " + getProvider().getCloudName());
            }
            Map<String,Object> json = new HashMap<>();

            json.put("pause", null);

            getMethod().postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void resume(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.resume");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new ResourceNotFoundException("No such virtual machine found for " + vmId);
            }
            if( !getCapabilities().supportsResume() ) {
                throw new OperationNotSupportedException("Suspend/resume is not supported in " + getProvider().getCloudName());
            }
            Map<String,Object> json = new HashMap<>();

            json.put("resume", null);

            getMethod().postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.start");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new ResourceNotFoundException("No such virtual machine found for " + vmId);
            }
            if( !getCapabilities().supportsStart() ) {
                throw new OperationNotSupportedException("Start/stop is not supported in " + getProvider().getCloudName());
            }
            Map<String,Object> json = new HashMap<>();

            json.put("os-start", null);

            getMethod().postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.stop");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new ResourceNotFoundException("No such virtual machine found for " + vmId);
            }
            if( !getCapabilities().supportsStop() ) {
                throw new OperationNotSupportedException("Start/stop is not supported in " + getProvider().getCloudName());
            }
            Map<String,Object> json = new HashMap<>();

            json.put("os-stop", null);

            getMethod().postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void suspend(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.suspend");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new ResourceNotFoundException("No such virtual machine found for " + vmId);
            }
            if( !getCapabilities().supportsSuspend() ) {
                throw new OperationNotSupportedException("Suspend/resume is not supported in " + getProvider().getCloudName());
            }
            Map<String,Object> json = new HashMap<>();

            json.put("suspend", null);

            getMethod().postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void unpause(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.unpause");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new ResourceNotFoundException("No such virtual machine found for " + vmId);
            }
            if( !getCapabilities().supportsUnPause() ) {
                throw new OperationNotSupportedException("Pause/unpause is not supported in " + getProvider().getCloudName());
            }
            Map<String,Object> json = new HashMap<>();

            json.put("unpause", null);

            getMethod().postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.reboot");
        try {
            Map<String,Object> json = new HashMap<>();
            Map<String,Object> action = new HashMap<>();

            action.put("type", "HARD");
            json.put("reboot", action);

            getMethod().postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void terminate(@Nonnull String vmId, @Nullable String explanation) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.terminate");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);
            if( vm == null) {
                return; // do nothing, machine is already gone
            }
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    Quantum quantum = getQuantum();
                    if( quantum != null ) {
                        String cachedPortId = (String) vm.getTag("org.dasein.portId");
                        Iterable<String> portIds = quantum.listPorts(vm);
                        for (String portId : portIds) {
                            quantum.removePort(portId);
                            if (portId.equalsIgnoreCase(cachedPortId)) {
                                cachedPortId = null;
                            }
                        }
                        // if ports were detached, listPorts will not return any ports, diff method to be used
                        if (cachedPortId != null) {
                            quantum.removePort(cachedPortId);
                        }
                    }
                    getMethod().deleteServers("/servers", vmId);
                    return;
                }
                catch( NovaException e ) {
                    if( e.getHttpCode() != HttpStatus.SC_CONFLICT ) {
                        throw e;
                    }
                }
                try { Thread.sleep(CalendarWrapper.MINUTE); }
                catch( InterruptedException e ) { /* ignore */ }
            } while( System.currentTimeMillis() < timeout );
        }
        finally {
            APITrace.end();
        }
    }

    protected  @Nullable VirtualMachineProduct toProduct(@Nullable JSONObject json) throws JSONException, InternalException, CloudException {
        if( json == null ) {
            return null;
        }
        VirtualMachineProduct product = new VirtualMachineProduct();

        if( json.has("id") ) {
            product.setProviderProductId(json.getString("id"));
        }
        if( json.has("name") ) {
            product.setName(json.getString("name"));
        }
        if( json.has("description") ) {
            product.setDescription(json.getString("description"));
        }
        if( json.has("ram") ) {
            product.setRamSize(new Storage<>(json.getInt("ram"), Storage.MEGABYTE));
        }
        if( json.has("disk") ) {
            product.setRootVolumeSize(new Storage<>(json.getInt("disk"), Storage.GIGABYTE));
        }
        product.setCpuCount(1);

        if( product.getProviderProductId() == null ) {
            return null;
        }
        if( product.getName() == null ) {
            product.setName(product.getProviderProductId());
        }
        if( product.getDescription() == null ) {
            product.setDescription(product.getName());
        }
        product.setArchitectures(Architecture.I32, Architecture.I64);
        return product;
    }

    protected  @Nullable ResourceStatus toStatus(@Nullable JSONObject server) throws JSONException, InternalException, CloudException {
        if( server == null ) {
            return null;
        }
        String serverId = null;

        if( server.has("id") ) {
            serverId = server.getString("id");
        }
        if( serverId == null ) {
            return null;
        }

        VmState state = VmState.PENDING;

        if( server.has("status") ) {
            String s = server.getString("status").toLowerCase();

            if( s.equals("active") ) {
                state = VmState.RUNNING;
            }
            else if( s.equals("build") ) {
                state = VmState.PENDING;
            }
            else if( s.equals("deleted") ) {
                state = VmState.TERMINATED;
            }
            else if( s.equals("suspended") ) {
                state = VmState.SUSPENDED;
            }
            else if( s.equalsIgnoreCase("paused") ) {
                state = VmState.PAUSED;
            }
            else if( s.equalsIgnoreCase("stopped") || s.equalsIgnoreCase("shutoff")) {
                state = VmState.STOPPED;
            }
            else if( s.equalsIgnoreCase("stopping") ) {
                state = VmState.STOPPING;
            }
            else if( s.equalsIgnoreCase("pausing") ) {
                state = VmState.PAUSING;
            }
            else if( s.equalsIgnoreCase("suspending") ) {
                state = VmState.SUSPENDING;
            }
            else if( s.equals("error") ) {
                state = VmState.ERROR;
            }
            else if( s.equals("reboot") || s.equals("hard_reboot") ) {
                state = VmState.REBOOTING;
            }
            else {
                logger.warn("toVirtualMachine(): Unknown server state: " + s);
                state = VmState.PENDING;
            }
        }
        return new ResourceStatus(serverId, state);
    }

    protected @Nullable VirtualMachine toVirtualMachine(@Nullable JSONObject server, @Nonnull Iterable<IpAddress> ipv4, @Nonnull Iterable<IpAddress> ipv6, @Nonnull Iterable<VLAN> networks) throws JSONException, InternalException, CloudException {
        if( server == null ) {
            return null;
        }
        VirtualMachine vm = new VirtualMachine();
        String description = null;

//        vm.setCurrentState(VmState.RUNNING);
        vm.setArchitecture(Architecture.I64);
        vm.setClonable(false);
        vm.setCreationTimestamp(-1L);
        vm.setImagable(false);
        vm.setLastBootTimestamp(-1L);
        vm.setLastPauseTimestamp(-1L);
        vm.setPausable(false);
        vm.setPersistent(true);
        vm.setPlatform(Platform.UNKNOWN);
        vm.setRebootable(true);
        vm.setProviderOwnerId(getTenantId());

        if (getCloudProvider().equals(OpenStackProvider.RACKSPACE)) {
            vm.setPersistent(false);
        }

        if( server.has("id") ) {
            vm.setProviderVirtualMachineId(server.getString("id"));
        }
        else return null;
        if( server.has("name") ) {
            vm.setName(server.getString("name"));
        }
        if( server.has("description") && !server.isNull("description") ) {
            description = server.getString("description");
        }
        if( server.has("kernel_id") ) {
            vm.setProviderKernelImageId(server.getString("kernel_id"));
        }
        if( server.has("ramdisk_id") ) {
            vm.setProviderRamdiskImageId(server.getString("ramdisk_id"));
        }
        JSONObject md = (server.has("metadata") && !server.isNull("metadata")) ? server.getJSONObject("metadata") : null;

        Map<String,String> map = new HashMap<>();
        boolean imaging = false;

        if( md != null ) {
            if( md.has("org.dasein.description") && vm.getDescription() == null ) {
                description = md.getString("org.dasein.description");
            }
            else if( md.has("Server Label") ) {
                description = md.getString("Server Label");
            }
            if( md.has("org.dasein.platform") ) {
                try {
                    vm.setPlatform(Platform.valueOf(md.getString("org.dasein.platform")));
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }
            String[] keys = JSONObject.getNames(md);

            if( keys != null ) {
                for( String key : keys ) {
                    String value = md.getString(key);

                    if( value != null ) {
                        map.put(key, value);
                    }
                }
            }
        }
        if( server.has("OS-EXT-STS:task_state") && !server.isNull("OS-EXT-STS:task_state") ) {
            String t = server.getString("OS-EXT-STS:task_state");

            map.put("OS-EXT-STS:task_state", t);
            imaging = t.equalsIgnoreCase("image_snapshot");
        }
        if( description == null ) {
            if( vm.getName() == null ) {
                vm.setName(vm.getProviderVirtualMachineId());
            }
            vm.setDescription(vm.getName());
        }
        else {
            vm.setDescription(description);
        }
        if( server.has("hostId") ) {
            map.put("host", server.getString("hostId"));
        }
        vm.setTags(map);
        if( server.has("image") && !server.isNull("image")) {
            try {
                JSONObject img = server.getJSONObject("image");

                if( img.has("id") ) {
                    vm.setProviderMachineImageId(img.getString("id"));
                }
            }
            catch (JSONException ex) {
                logger.error("Unable to parse the image object");
                try {
                    server.getString("image");
                    logger.error("Image object has been returned as a string from cloud "+server.getString("image"));
                }
                catch (JSONException ignore) {}
            }
        }
        if( server.has("flavor") ) {
            JSONObject f = server.getJSONObject("flavor");

            if( f.has("id") ) {
                vm.setProductId(f.getString("id"));
            }
        }
        else if( server.has("flavorId") ) {
            vm.setProductId(server.getString("flavorId"));
        }
        if( server.has("adminPass") ) {
            vm.setRootPassword(server.getString("adminPass"));
        }
        if( server.has("key_name") ) {
            vm.setProviderShellKeyIds(server.getString("key_name"));
        }
        if( server.has("status") ) {
            String s = server.getString("status").toLowerCase();

            if( s.equals("active") ) {
                vm.setCurrentState(VmState.RUNNING);
            }
            else if( s.startsWith("build") ) {
                vm.setCurrentState(VmState.PENDING);
            }
            else if( s.equals("deleted") ) {
                vm.setCurrentState(VmState.TERMINATED);
            }
            else if( s.equals("suspended") ) {
                vm.setCurrentState(VmState.SUSPENDED);
            }
            else if( s.equalsIgnoreCase("paused") ) {
                vm.setCurrentState(VmState.PAUSED);
            }
            else if( s.equalsIgnoreCase("stopped") || s.equalsIgnoreCase("shutoff")) {
                vm.setCurrentState(VmState.STOPPED);
            }
            else if( s.equalsIgnoreCase("stopping") ) {
                vm.setCurrentState(VmState.STOPPING);
            }
            else if( s.equalsIgnoreCase("pausing") ) {
                vm.setCurrentState(VmState.PAUSING);
            }
            else if( s.equalsIgnoreCase("suspending") ) {
                vm.setCurrentState(VmState.SUSPENDING);
            }
            else if( s.equals("error") ) {
                vm.setCurrentState(VmState.ERROR);
            }
            else if( s.equals("reboot") || s.equals("hard_reboot") ) {
                vm.setCurrentState(VmState.REBOOTING);
            }
            else {
                logger.warn("toVirtualMachine(): Unknown server state: " + s);
                vm.setCurrentState(VmState.PENDING);
            }
        }
        if( vm.getCurrentState() == null && imaging ) {
            vm.setCurrentState(VmState.PENDING);
        }
        if( server.has("created") ) {
            vm.setCreationTimestamp(NovaOpenStack.parseTimestamp(server.getString("created")));
        }
        if( server.has("addresses") ) {
            JSONObject addrs = server.getJSONObject("addresses");
            String[] names = JSONObject.getNames(addrs);

            if( names != null && names.length > 0 ) {
                List<RawAddress> pub = new ArrayList<>();
                List<RawAddress> priv = new ArrayList<>();

                for( String name : names ) {
                    JSONArray arr = addrs.getJSONArray(name);

                    String subnet = null;
                    for( int i=0; i<arr.length(); i++ ) {
                        RawAddress addr = null;
                        String type = null;

                        if( getMinorVersion() == 0 && getMajorVersion() == 1 ) {
                            addr = new RawAddress(arr.getString(i).trim(), IPVersion.IPV4);
                        }
                        else {
                            JSONObject a = arr.getJSONObject(i);
                            type = a.optString("OS-EXT-IPS:type");

                            if( a.has("version") && a.getInt("version") == 4 && a.has("addr") ) {
                                subnet = a.getString("addr");
                                addr = new RawAddress(a.getString("addr"), IPVersion.IPV4);
                            }
                            else if( a.has("version") && a.getInt("version") == 6 && a.has("addr") ) {
                                subnet = a.getString("addr");
                                addr = new RawAddress(a.getString("addr"), IPVersion.IPV6);
                            }
                        }
                        if( addr != null ) {
                            if ( "public".equalsIgnoreCase(name) || "internet".equalsIgnoreCase(name)) {
                                    pub.add(addr);
                            }
                            else if ("floating".equalsIgnoreCase(type)) {
                                pub.add(addr);
                            }
                            else if ("fixed".equalsIgnoreCase(type)) {
                                priv.add(addr);
                            }
                            else if( addr.isPublicIpAddress() ) {
                                pub.add(addr);
                            }
                            else {
                                priv.add(addr);
                            }
                        }
                    }
                    if( vm.getProviderVlanId() == null ) { // && !name.equals("public") && !name.equals("private") && !name.equals("nova_fixed") ) {
                        for( VLAN network : networks ) {
                            if( network.getName().equals(name) ) {
                                vm.setProviderVlanId(network.getProviderVlanId());
                                //get subnet
                                NetworkServices services = getProvider().getNetworkServices();
                                VLANSupport support = services.getVlanSupport();
                                Iterable<Subnet> subnets = support.listSubnets(network.getProviderVlanId());
                                for (Subnet sub : subnets) {
                                    try {
                                        SubnetUtils utils = new SubnetUtils(sub.getCidr());

                                        if( utils.getInfo().isInRange(subnet) ) {
                                            vm.setProviderSubnetId(sub.getProviderSubnetId());
                                            break;
                                        }
                                    }
                                    catch( IllegalArgumentException arg ) {
                                        logger.warn("Couldn't match against an invalid CIDR: "+sub.getCidr());
                                        continue;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                vm.setPublicAddresses(pub.toArray(new RawAddress[pub.size()]));
                vm.setPrivateAddresses(priv.toArray(new RawAddress[priv.size()]));
            }
            RawAddress[] raw = vm.getPublicAddresses();

            if( raw != null ) {
                for( RawAddress addr : vm.getPublicAddresses() ) {
                    if( addr.getVersion().equals(IPVersion.IPV4) ) {
                        for( IpAddress a : ipv4 ) {
                            if( a.getRawAddress().getIpAddress().equals(addr.getIpAddress()) ) {
                                vm.setProviderAssignedIpAddressId(a.getProviderIpAddressId());
                                break;
                            }
                        }
                    }
                    else if( addr.getVersion().equals(IPVersion.IPV6) ) {
                        for( IpAddress a : ipv6 ) {
                            if( a.getRawAddress().getIpAddress().equals(addr.getIpAddress()) ) {
                                vm.setProviderAssignedIpAddressId(a.getProviderIpAddressId());
                                break;
                            }
                        }
                    }
                }
            }
            if( vm.getProviderAssignedIpAddressId() == null ) {
                for( IpAddress addr : ipv4 ) {
                    String serverId = addr.getServerId();

                    if( serverId != null && serverId.equals(vm.getProviderVirtualMachineId()) ) {
                        vm.setProviderAssignedIpAddressId(addr.getProviderIpAddressId());
                        break;
                    }
                }
                if( vm.getProviderAssignedIpAddressId() == null ) {
                    for( IpAddress addr : ipv6 ) {
                        String serverId = addr.getServerId();

                        if( serverId != null && addr.getServerId().equals(vm.getProviderVirtualMachineId()) ) {
                            vm.setProviderAssignedIpAddressId(addr.getProviderIpAddressId());
                            break;
                        }
                    }
                }
            }
            if( vm.getProviderAssignedIpAddressId() == null ) {
                for( IpAddress addr : ipv6 ) {
                    if( addr.getServerId().equals(vm.getProviderVirtualMachineId()) ) {
                        vm.setProviderAssignedIpAddressId(addr.getProviderIpAddressId());
                        break;
                    }
                }
            }
        }
        vm.setProviderRegionId(getRegionId());
        vm.setProviderDataCenterId(vm.getProviderRegionId() + "-a");
        vm.setTerminationTimestamp(-1L);
        if( vm.getName() == null ) {
            vm.setName(vm.getProviderVirtualMachineId());
        }
        if( vm.getDescription() == null ) {
            vm.setDescription(vm.getName());
        }

        if( Platform.UNKNOWN.equals(vm.getPlatform())) {
            vm.setPlatform(getPlatform(vm.getName(), vm.getDescription(), vm.getProviderMachineImageId()));
        }
        vm.setImagable(vm.getCurrentState() == null);
        vm.setRebootable(vm.getCurrentState() == null);

        if (getCloudProvider().equals(OpenStackProvider.RACKSPACE)) {
            //Rackspace does not support the concept for firewalls in servers
        	vm.setProviderFirewallIds(null);
        }
        else{
            Iterable<String> fwIds = listFirewalls(vm.getProviderVirtualMachineId(), server);
            int count = 0;

            //noinspection UnusedDeclaration
            for( String id : fwIds ) {
                count++;
            }
            String[] ids = new String[count];
            int i = 0;

            for( String id : fwIds ) {
                ids[i++] = id;
            }
            vm.setProviderFirewallIds(ids);
        }
        return vm;
    }
    
    @Override
    public void setTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Server.setTags");
    	try {
    		getProvider().createTags( SERVICE, "/servers", vmId, tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void setTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	for( String id : vmIds ) {
    		setTags(id, tags);
    	}
    }
    
    @Override
    public void updateTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Server.updateTags");
    	try {
    		getProvider().updateTags( SERVICE, "/servers", vmId, tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void updateTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	for( String id : vmIds ) {
    		updateTags(id, tags);
    	}
    }
    
    @Override
    public void removeTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Server.removeTags");
    	try {
    		getProvider().removeTags( SERVICE, "/servers", vmId, tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void removeTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	for( String id : vmIds ) {
    		removeTags(id, tags);
    	}
    }

}
