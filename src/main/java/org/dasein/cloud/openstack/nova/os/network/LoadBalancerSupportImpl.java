package org.dasein.cloud.openstack.nova.os.network;

import org.apache.commons.net.util.SubnetUtils;
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

/**
 * Created by mariapavlova on 23/04/2015.
 */

/**
 *
 * CAVEATS:
 *
 * - OS LB 1.0 doesn't appear to support SSL certificates management, therefore we don't announce
 * support for HTTPS load-balancing
 *
 * - OS LB supports balancing off one public port to individual private ports which can be different on each
 * of the endpoints. Dasein model doesn't support this, so we have to limit it. The LbListener private port is
 * currently serialized into LbListener name like this PoolId:PrivatePort. This will break if name is edited
 * in the direct console.
 *
 * - OS LB 1.0 only supports one VIP per pool, which in Dasein terms means we can only support one LbListener per
 * LoadBalancer, which has to be created upon LoadBalancer create. We do not allow removal of LbListeners.
 *
 * - Since LB OS 1.0 is not announced through service catalogue, we simply assume it is available via Neutron's
 * endpoint, LBaaS should theoretically be announced via the service catalogue, which will make things cleaner.
 *
 * - OS LB 1.0 supports multiple health monitors per loadbalancer, Dasein only supports a single health monitor.
 *
 *
 * ENTITY MAP:
 *
 * OS Pool -&gt; Dasein LoadBalancer
 *
 * OS VIP -&gt; Dasein LbListener
 *
 * OS Member -&gt; Dasein LoadBalancerEndpoint
 *
 * OS Health Monitor -&gt; Dasein LB Health Check
 *
 */
public class LoadBalancerSupportImpl extends AbstractLoadBalancerSupport<NovaOpenStack> {

    static private final Logger logger = Logger.getLogger(LoadBalancerSupportImpl.class);

    private volatile transient LoadBalancerCapabilitiesImpl capabilities;

    LoadBalancerSupportImpl(NovaOpenStack provider) {
        super(provider);
    }

    @Override
    public @Nonnull LoadBalancerCapabilities getCapabilities() throws CloudException, InternalException {
        if (capabilities == null) {
            capabilities = new LoadBalancerCapabilitiesImpl(getProvider());
        }
        return capabilities;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.isSubscribed");
        try {
            listLoadBalancers();
            return true;
        }
        catch( CloudException e ) {
            return false;
        }
    }

    @Override
    public @Nonnull String createLoadBalancer(@Nonnull LoadBalancerCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.createLoadBalancer");
        try {
            Map<String,Object> lb = new HashMap<String,Object>();

            lb.put("name", options.getName());
            lb.put("description", options.getDescription());
            lb.put("tenant_id", getAccountNumber());
            String subnetId;
            // The subnet selection logic below is a bit iffy, but it will do for the first pass
            if( options.getProviderSubnetIds() != null && options.getProviderSubnetIds().length > 0 ) {
                subnetId = options.getProviderSubnetIds()[0];
            }
            else {
                Iterable<Subnet> subnets = getProvider().getNetworkServices().getVlanSupport().listSubnets(options.getProviderVlanId());
                if( !subnets.iterator().hasNext() ) {
                    throw new InternalException("Provided VLAN ("+options.getProviderVlanId()+") does not have any subnets defined. "
                            + getProvider().getCloudName() + " requires a subnet upon loadbalancer create.");
                }
                subnetId = subnets.iterator().next().getProviderSubnetId();
            }
            lb.put("subnet_id", subnetId);

            // Neutron loadbalancers only support one VIP per pool, which means one listener per LB in Dasein terms
            if(  options.getListeners() == null || options.getListeners().length != getCapabilities().getMaxPublicPorts() ) {
                throw new InternalException(getProvider().getCloudName() + " requires " + getCapabilities().getMaxPublicPorts() + " listener(s) to be specified upon load balancer create");
            }

            LbListener listener = options.getListeners()[0]; // the only possible listener in Neutron

            lb.put("lb_method", toOSAlgorithm(listener.getAlgorithm()));
            lb.put("protocol", toOSProtocol(listener.getNetworkProtocol()));

            Map<String,Object> json = new HashMap<String,Object>();
            json.put("pool", lb);

            if( logger.isTraceEnabled() ) {
                logger.trace("create(): Posting new load balancer data...");
            }
            JSONObject result = getMethod().postNetworks(getLoadBalancersResource(), null, new JSONObject(json), false);

            if( result == null ) {
                logger.error("create(): Method executed successfully, but no load balancer was created");
                throw new GeneralCloudException("No loadbalancer was created", CloudErrorType.GENERAL);
            }
            try{
                if( result.has("pool") ) {
                    JSONObject ob = result.getJSONObject("pool");
                    if( ob != null ) {
                        String lbId = ob.getString("id");
                        try {
                            createListener(lbId, subnetId, listener);
                        }
                        catch ( Throwable e ) {
                            getMethod().deleteNetworks(getLoadBalancersResource(), lbId);
                            throw new GeneralCloudException("No listener was created", CloudErrorType.GENERAL);

                        }
                        if( options.getEndpoints() != null ) {
                            for( LoadBalancerEndpoint endpoint : options.getEndpoints() ) {
                                createMember(lbId, endpoint.getEndpointValue(), listener.getPrivatePort());
                            }
                        }
                        if( options.getHealthCheckOptions() != null ) {
                            createHealthMonitor(Collections.singletonList(lbId), options.getHealthCheckOptions());
                        }
                        return lbId;
                    }
                }
                logger.error("create(): Method executed successfully, but no load balancer was found in JSON");
                throw new GeneralCloudException("Method executed successfully, but no load balancer was found in JSON", CloudErrorType.GENERAL);
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

    @Override
    public @Nonnull Iterable<ResourceStatus> listLoadBalancerStatus() throws CloudException, InternalException {
        // Unlike Horizon the OS LB API returns all tenants' load balancers, so we must filter
        JSONObject result = getMethod().getNetworks(getLoadBalancersResource(), null, false,
                "?tenant_id="+ getAccountNumber() + "&fields=id&fields=status");
        List<ResourceStatus> results = new ArrayList<ResourceStatus>();
        if( result != null && result.has("pools") ) {
            try {
                JSONArray pools = result.getJSONArray("pools");
                for( int i = 0; i < pools.length(); i++ ) {
                    JSONObject lb = pools.getJSONObject(i);
                    results.add(new ResourceStatus(lb.getString("id"), lb.getString("status")));
                }
            }
            catch( JSONException e ) {
                throw new CommunicationException("Unable to parse list load balancers response" + e.getMessage(), e);

            }
        }
        return results;
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints(@Nonnull String forLoadBalancerId) throws CloudException, InternalException {
        List<JSONObject> members = findAllMembers(forLoadBalancerId);
        List<LoadBalancerEndpoint> endpoints = new ArrayList<LoadBalancerEndpoint>();
        for( JSONObject member : members ) {
            endpoints.add(toLoadBalancerEnpoint(member));
        }
        return endpoints;
    }

    @Override
    public void addIPEndpoints(@Nonnull String toLoadBalancerId, @Nonnull String... ipAddresses) throws CloudException, InternalException {
        LoadBalancer lb = getLoadBalancer(toLoadBalancerId);
        if (lb == null) {
            throw new InternalException("Load balancer not found " + toLoadBalancerId);
        }
        if (lb.getListeners() == null || lb.getListeners().length == 0) {
            throw new InternalException("Listeners are not defined for load balancer " + toLoadBalancerId);
        }
        int privatePort = lb.getListeners()[0].getPrivatePort();
        for (String ipAddress : ipAddresses) {
            createMember(toLoadBalancerId, ipAddress, privatePort);
        }
    }

    private Subnet findSubnetById(List<Subnet> subnets, String subnetId) {
        for( Subnet subnet : subnets ) {
            if( subnetId.equalsIgnoreCase(subnet.getProviderSubnetId()) ) {
                return subnet;
            }
        }
        return null;
    }

//    @Override
//    public void addServers(@Nonnull String toLoadBalancerId, @Nonnull String... serverIdsToAdd) throws CloudException, InternalException {
//        LoadBalancer lb = getLoadBalancer(toLoadBalancerId);
//        List<SubnetUtils.SubnetInfo> ranges = new ArrayList<SubnetUtils.SubnetInfo>();
//        List<Subnet> allSubnets = new ArrayList<Subnet>();
//        for( VLAN vlan : getProvider().getNetworkServices().getVlanSupport().listVlans() ) {
//            Iterator<Subnet> vlanSubnets = getProvider().getNetworkServices().getVlanSupport().listSubnets(vlan.getProviderVlanId()).iterator();
//            while( vlanSubnets.hasNext() ) {
//                allSubnets.add(vlanSubnets.next());
//            }
//        }
//        if( lb.getProviderSubnetIds() != null ) {
//            for( String subnetId : lb.getProviderSubnetIds() ) {
//                Subnet subnet = findSubnetById(allSubnets, subnetId);
//                ranges.add(new SubnetUtils(subnet.getCidr()).getInfo());
//            }
//        }
//        List<String> ipAddressesToAdd = new ArrayList<String>();
//        for( String serverId : serverIdsToAdd ) {
//            VirtualMachine vm = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(serverId);
//            if( vm != null ) {
//                for(RawAddress address : vm.getPrivateAddresses() ) {
//                    for( SubnetUtils.SubnetInfo range : ranges ) {
//                        if( range.isInRange(address.getIpAddress()) ) {
//                            ipAddressesToAdd.add(address.getIpAddress());
//                            break;
//                        }
//                    }
//                }
//            }
//        }
//        if( ipAddressesToAdd.isEmpty() ) {
//            throw new InternalException("None of the requested server ids match to subnets assigned to load balancer");
//        }
//        addIPEndpoints(toLoadBalancerId, ipAddressesToAdd.toArray(new String[ipAddressesToAdd.size()]));
//    }

    protected NovaMethod getMethod() {
        return new NovaMethod(getProvider());
    }

    @Override
    public void removeIPEndpoints(@Nonnull String fromLoadBalancerId, @Nonnull String... addresses) throws CloudException, InternalException {
        for( JSONObject member : findAllMembers(fromLoadBalancerId) ) {
            for( String address : addresses ) {
                try {
                    if( address.equals(member.getString("address")) ) {
                        getMethod().deleteNetworks(getMembersResource(), member.getString("id"));
                    }
                }
                catch( JSONException e ) {
                    throw new CommunicationException("Unable to parse response", e);
                }
            }
        }
    }

    protected VirtualMachine getVirtualMachine(String serverId) throws CloudException, InternalException {
        return getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(serverId);
    }

    @Override
    public void removeServers(@Nonnull String fromLoadBalancerId, @Nonnull String... serverIdsToRemove) throws CloudException, InternalException {
        List<String> ipAddressesToRemove = new ArrayList<>();
        for( String serverId : serverIdsToRemove ) {
            VirtualMachine vm = getVirtualMachine(serverId);
            if( vm != null ) {
                for( RawAddress address : vm.getPrivateAddresses() ) {
                    ipAddressesToRemove.add(address.getIpAddress());
                }
            }
        }
        removeIPEndpoints(fromLoadBalancerId, ipAddressesToRemove.toArray(new String[ipAddressesToRemove.size()]));

    }

    @Override
    public LoadBalancerHealthCheck createLoadBalancerHealthCheck(@Nullable String name, @Nullable String description, @Nullable String host, @Nullable LoadBalancerHealthCheck.HCProtocol protocol, int port, @Nullable String path, int interval, int timeout, int healthyCount, int unhealthyCount) throws CloudException, InternalException {
        HealthCheckOptions opts = HealthCheckOptions.getInstance(name, description, null, host, protocol, port, path, interval, timeout, healthyCount, unhealthyCount);
        return createLoadBalancerHealthCheck(opts);
    }

    @Override
    public LoadBalancerHealthCheck createLoadBalancerHealthCheck(@Nonnull HealthCheckOptions options) throws CloudException, InternalException {
        return createHealthMonitor(Collections.singletonList(options.getProviderLoadBalancerId()), options);
    }

    @Override
    public void attachHealthCheckToLoadBalancer(@Nonnull String providerLoadBalancerId, @Nonnull String providerLBHealthCheckId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.attachHealthCheckToLoadBalancer");

        try {
            Map<String, Object> lb = new HashMap<String, Object>();
            lb.put("id", providerLBHealthCheckId);
            Map<String, Object> json = new HashMap<String, Object>();

            json.put("health_monitor", lb);

            if( logger.isTraceEnabled() ) {
                logger.trace("create(): Posting health monitor id to attach...");
            }
            JSONObject result = getMethod().postNetworks(getLoadBalancersResource(), providerLoadBalancerId, new JSONObject(json), "health_monitors");
            if( result == null ) {
                logger.error("create(): Method executed successfully, but no health monitor was attached");
                throw new GeneralCloudException("Method executed successfully, but no health monitor was attached", CloudErrorType.GENERAL);

            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<LoadBalancerHealthCheck> listLBHealthChecks(@Nullable HealthCheckFilterOptions opts) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.listLBHealthChecks");
        try {
            JSONObject result = getMethod().getNetworks(getHealthMonitorsResource(), null, false);
            List<LoadBalancerHealthCheck> healthMonitors = new ArrayList<LoadBalancerHealthCheck>();
            if( result != null && result.has("health_monitors") ) {
                try {
                    JSONArray list = result.getJSONArray("health_monitors");
                    for( int i = 0; i < list.length(); i++ ) {
                        LoadBalancerHealthCheck lbhc = toLoadBalancerHealthCheck(list.getJSONObject(i));
                        if( opts == null || opts.matches(lbhc)) {
                            healthMonitors.add(lbhc);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("Unable to identify expected values in JSON:" + e.getMessage());
                    throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
                }
            }
            return healthMonitors;
        }
        finally {
            APITrace.end();
        }

    }

    @Override
    public LoadBalancerHealthCheck getLoadBalancerHealthCheck(@Nonnull String providerLBHealthCheckId, /* ignored */ @Nullable String providerLoadBalancerId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.getLoadBalancerHealthCheck");
        try {

            JSONObject result = getMethod().getNetworks(getHealthMonitorsResource(), providerLBHealthCheckId, false);

            if( result == null ) {
                logger.error("create(): Method executed successfully, but no health monitor was found");
                throw new GeneralCloudException("Method executed successfully, but no health monitor was found", CloudErrorType.GENERAL);

            }
            try {
                return toLoadBalancerHealthCheck(result.getJSONObject("health_monitor"));
            }
            catch( JSONException e ) {
                throw new CommunicationException("Unable to parse a health check object " + e.getMessage(), e);

            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public LoadBalancerHealthCheck modifyHealthCheck(@Nonnull String providerLBHealthCheckId, @Nonnull HealthCheckOptions opt) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "LB.modifyHealthCheck");

        try {
            // CAVEAT: these are the only three attributes that aren't read-only in OS LB, so let's delete and re-create
            LoadBalancerHealthCheck hc = getLoadBalancerHealthCheck(providerLBHealthCheckId, null);
            safeDeleteLoadBalancerHealthCheck(hc);
            return createHealthMonitor(hc.getProviderLoadBalancerIds(), opt);
        }
        finally {
            APITrace.end();
        }
    }

    protected void safeDeleteLoadBalancerHealthCheck(LoadBalancerHealthCheck hc) throws CloudException, InternalException {
        for( String loadBalancer: hc.getProviderLoadBalancerIds() ) {
            detatchHealthCheck(loadBalancer, hc.getProviderLBHealthCheckId());
        }
        deleteHealthMonitor(hc.getProviderLBHealthCheckId());
    }

    @Override
    public void removeLoadBalancerHealthCheck(@Nonnull String providerLoadBalancerId) throws CloudException, InternalException {
        Iterator<LoadBalancerHealthCheck> healthChecks = listLBHealthChecks(null).iterator();
        while( healthChecks.hasNext() ) {
            LoadBalancerHealthCheck hc = healthChecks.next();
            if( hc.getProviderLoadBalancerIds().contains(providerLoadBalancerId) ) {
                safeDeleteLoadBalancerHealthCheck(hc);
            }
        }
    }

    public void detatchHealthCheck(@Nonnull String loadBalancerId, @Nonnull String providerLBHealthCheckId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.deleteHealthMonitor");
        try {
            new NovaMethod(getProvider()).deleteNetworks(getLoadBalancersResource(), loadBalancerId + "/health_monitors/"+providerLBHealthCheckId);
        }
        finally {
            APITrace.end();
        }
    }

    protected LoadBalancerHealthCheck createHealthMonitor(@Nonnull List<String> loadbalancerIds, @Nonnull HealthCheckOptions opt) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "LB.createListener");

        try {
            Map<String, Object> lb = new HashMap<String, Object>();
            lb.put("tenant_id", getAccountNumber());
            lb.put("type", toOSHCType(opt.getProtocol()));
            lb.put("delay", opt.getInterval());
            lb.put("timeout", opt.getTimeout());
            lb.put("max_retries", opt.getUnhealthyCount());
            // the opt.getPort() is ignored since OS is using the servers' private ports automatically
            if( opt.getPath() != null ) {
                lb.put("url_path", opt.getPath());
            }
            Map<String, Object> json = new HashMap<String, Object>();

            json.put("health_monitor", lb);

            if( logger.isTraceEnabled() ) {
                logger.trace("create(): Posting new health monitor data...");
            }
            JSONObject result = getMethod().postNetworks(getHealthMonitorsResource(), null, new JSONObject(json), null);

            if( result == null ) {
                logger.error("create(): Method executed successfully, but no health monitor was created");
                throw new GeneralCloudException("Method executed successfully, but no health monitor was created", CloudErrorType.GENERAL);

            }
            LoadBalancerHealthCheck hc = toLoadBalancerHealthCheck(result.getJSONObject("health_monitor"));
            if( hc == null ) {
                throw new GeneralCloudException("Unable to create loadbalancer health check" , CloudErrorType.GENERAL);
            }
            for( String lbId : loadbalancerIds ) {
                attachHealthCheckToLoadBalancer(lbId, hc.getProviderLBHealthCheckId());
            }
            return hc;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to parse create health monitor response" + e.getMessage(), e);

        }
        finally {
            APITrace.end();
        }

    }

    protected String getAccountNumber() throws InternalException {
        return getContext().getAccountNumber();
    }

    protected String getCurrentRegionId() throws InternalException {
        return getContext().getRegionId();
    }

    protected void deleteHealthMonitor(String providerLBHealthCheckId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.deleteHealthMonitor");
        try {
            getMethod().deleteNetworks(getHealthMonitorsResource(), providerLBHealthCheckId);
        }
        finally {
            APITrace.end();
        }
    }

    /**
     * Convert OpenStack health_monitor object to Dasein LoadBalancerHealthCheck
     * @param ob
     * @return LoadBalancerHealthCheck
     * @throws JSONException
     */
    protected @Nonnull LoadBalancerHealthCheck toLoadBalancerHealthCheck(JSONObject ob) throws JSONException, InternalException, CloudException {
        LoadBalancerHealthCheck.HCProtocol protocol = fromOSProtocol(ob.getString("type"));
        int count = ob.getInt("max_retries");
        int port = -1;
        String path = ob.optString("url_path", null);
        JSONArray pools = ob.optJSONArray("pools");
        for( int i=0; i<pools.length(); i++ ) {
            String lbId = pools.getJSONObject(i).getString("pool_id");
            LoadBalancer lb = getLoadBalancer(lbId);
            if( lb != null ) {
                LbListener[] listeners = lb.getListeners();
                if( listeners != null && listeners.length > 0 ) {
                    port = listeners[0].getPrivatePort();
                    break;
                }
            }
        }
        String id = ob.getString("id");
        int timeout = ob.getInt("timeout");
        int interval = ob.getInt("delay");
        LoadBalancerHealthCheck lbhc = LoadBalancerHealthCheck.getInstance(id, protocol, port, path, interval, timeout, count, count);
        for( int i=0; i<pools.length(); i++ ) {
            lbhc.addProviderLoadBalancerId(pools.getJSONObject(i).getString("pool_id"));
        }
        return lbhc;
    }

    protected String toOSHCType(LoadBalancerHealthCheck.HCProtocol hcProtocol) {
        switch( hcProtocol ) {
            case TCP:
                return "TCP";
            case HTTPS:
                return "HTTPS";
            default:
                return "HTTP";
        }
    }

    protected void createListener(@Nonnull String lbId, String subnetId, @Nonnull LbListener listener) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.createListener");

        try {
            Map<String, Object> lb = new HashMap<String, Object>();
            // CAVEAT: there's no way to serialise Dasein listener's private port in the VIP,
            // private ports in OS are set in members which may not and unlikely to even exist at this point
            // so we have to serialise the private port into the VIP name.
            lb.put("name", generateListenerId(lbId, listener.getPrivatePort()));
            lb.put("tenant_id", getAccountNumber());
            lb.put("protocol", toOSProtocol(listener.getNetworkProtocol()));
            lb.put("subnet_id", subnetId);
            lb.put("protocol_port", listener.getPublicPort());
            lb.put("pool_id", lbId);
            Map<String, Object> json = new HashMap<String, Object>();

            json.put("vip", lb);

            if( logger.isTraceEnabled() ) {
                logger.trace("create(): Posting new listener data...");
            }
            JSONObject result = getMethod().postNetworks(getListenersResource(), null, new JSONObject(json), false);

            if( result == null ) {
                logger.error("create(): Method executed successfully, but no listener was created");
                throw new GeneralCloudException("No listener was created", CloudErrorType.GENERAL);

            }
        }
        finally {
            APITrace.end();
        }
    }

    protected void createMember(@Nonnull String lbId, String address, int privatePort) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.createMember");
        try {
            Map<String, Object> lb = new HashMap<String, Object>();

            lb.put("tenant_id", getAccountNumber());
            lb.put("protocol_port", privatePort);
            lb.put("address", address);
            lb.put("pool_id", lbId);
            Map<String, Object> json = new HashMap<String, Object>();

            json.put("member", lb);

            if( logger.isTraceEnabled() ) {
                logger.trace("create(): Posting new listener data...");
            }
            JSONObject result = getMethod().postNetworks(getMembersResource(), null, new JSONObject(json), false);
            if( result == null ) {
                logger.error("create(): Method executed successfully, but no load balancer member was created");
                throw new GeneralCloudException("Method executed successfully, but no load balancer member was created", CloudErrorType.GENERAL);

            }
        }
        finally {
            APITrace.end();
        }
    }

    /**
     * Find all VIP
     * @param loadBalancerId optional filter
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    protected List<JSONObject> findAllVips(@Nullable String loadBalancerId) throws CloudException, InternalException {
        JSONObject result = getMethod().getNetworks(getListenersResource(), null, false, "?tenant_id="+ getAccountNumber());
        List<JSONObject> listeners = new ArrayList<JSONObject>();
        if( result != null && result.has("vips") ) {
            try {
                JSONArray list = result.getJSONArray("vips");
                for( int i = 0; i < list.length(); i++ ) {
                    JSONObject vip = list.getJSONObject(i);
                    if( loadBalancerId == null || loadBalancerId.equalsIgnoreCase(vip.getString("pool_id")) ) {
                        listeners.add(vip);
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to understand listVips response: " + e.getMessage());
                throw new CommunicationException("Unable to understand listVips response: " + e.getMessage(), e);
            }
        }
        return listeners;
    }

    /**
     * Find all members
     * @param loadBalancerId optional filter
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    protected List<JSONObject> findAllMembers(@Nullable String loadBalancerId) throws CloudException, InternalException {
        JSONObject result = getMethod().getNetworks(getMembersResource(), null, false, "?tenant_id="+ getAccountNumber());
        List<JSONObject> members = new ArrayList<JSONObject>();
        if( result != null && result.has("members") ) {
            try {
                JSONArray list = result.getJSONArray("members");
                for( int i = 0; i < list.length(); i++ ) {
                    JSONObject member = list.getJSONObject(i);
                    if( (loadBalancerId != null && loadBalancerId.equals(member.optString("pool_id")) )
                        || loadBalancerId == null) {
                        members.add(member);
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to understand listMembers response: " + e.getMessage());
                throw new CommunicationException("Unable to understand listMembers response: " + e.getMessage(), e);
            }
        }
        return members;
    }

    /**
     * Find all or a single load balancer
     * @param loadBalancerId optional load balancer id if looking for just one
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    protected List<LoadBalancer> findLoadBalancers(@Nullable String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.listLoadBalancers");
        try {
            // get all vips, optionally filtered by lbId
            List<JSONObject> listeners = findAllVips(loadBalancerId);

            // get all members, optionally filtered by lbId
            List<JSONObject> members = findAllMembers(loadBalancerId);

            // Unlike Horizon the OS LB API returns all tenants' load balancers, so we must filter
            JSONObject result = getMethod().getNetworks(getLoadBalancersResource(), loadBalancerId, false, "?tenant_id="+ getAccountNumber());
            List<LoadBalancer> results = new ArrayList<LoadBalancer>();
            if( loadBalancerId == null && result != null && result.has("pools") ) {
                try {
                    JSONArray loadbalancers = result.getJSONArray("pools");
                    for( int i = 0; i < loadbalancers.length(); i++ ) {
                        JSONObject lb = loadbalancers.getJSONObject(i);
                        results.add(toLoadBalancer(lb, listeners, members));
                    }
                }
                catch( JSONException e ) {
                    logger.error("Unable to understand listPools response: " + e.getMessage());
                    throw new GeneralCloudException("Unable to understand listPools response: ", CloudErrorType.GENERAL);
                }
            }
            else if( result != null && result.has("pool")) {
                try {
                    return Collections.singletonList(toLoadBalancer(result.getJSONObject("pool"), listeners, members));
                }
                catch( JSONException e ) {
                    logger.error("Unable to understand getPool response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand getPool response: " + e.getMessage(), e);
                }
            }

            return results;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        return findLoadBalancers(null);
    }


    protected @Nullable JSONObject findListenerByLbId(@Nonnull List<JSONObject> listeners, @Nonnull String lbId) throws JSONException {
        for( JSONObject listener : listeners ) {
            if( listener.has("pool_id") && lbId.equals(listener.getString("pool_id")) ) {
                return listener;
            }
        }
        return null;
    }

    protected LoadBalancer toLoadBalancer(JSONObject lb, List<JSONObject> listeners, List<JSONObject> members) throws JSONException, InternalException {
        String ownerId = lb.optString("tenant_id");
        String regionId = getCurrentRegionId();
        String lbId = lb.getString("id");
        LoadBalancerState state = "ACTIVE".equalsIgnoreCase(lb.getString("status"))
                ? LoadBalancerState.ACTIVE : LoadBalancerState.PENDING;
        String name = lb.getString("name");
        String description = lb.getString("description");
        LoadBalancerAddressType addressType = LoadBalancerAddressType.IP;
        JSONObject vip = findListenerByLbId(listeners, lbId);
        String address = null;
        int publicPort = -1;
        LbAlgorithm algorithm = LbAlgorithm.ROUND_ROBIN;
        String poolAlgorithm = lb.getString("lb_method");
        if("LEAST_CONNECTIONS".equalsIgnoreCase(poolAlgorithm)) {
            algorithm = LbAlgorithm.LEAST_CONN;
        }
        else if("SOURCE_IP".equalsIgnoreCase(poolAlgorithm)) {
            algorithm = LbAlgorithm.SOURCE;
        }
        List <LbListener> lbListeners = new ArrayList<LbListener>();
        if( vip != null ) {
            String vipName = vip.getString("name");
            int privatePort = -1;
            String[] nameParts = vipName.split(":");
            if( nameParts.length == 2 ) {
                try {
                    privatePort = Integer.parseInt(nameParts[1]);
                }
                catch (NumberFormatException e) {}
            }
            address = vip.optString("address");
            LbProtocol protocol = LbProtocol.HTTP;
            String vipProtocol = vip.optString("protocol");
            if ("HTTPS".equalsIgnoreCase(vipProtocol)) {
                protocol = LbProtocol.HTTPS;
            }
            else if ("TCP".equalsIgnoreCase(vipProtocol)) {
                protocol = LbProtocol.RAW_TCP;
            }
            LbPersistence persistence = LbPersistence.NONE;
            if (vip.optJSONObject("session_persistence") != null) {
                String sessionPersistence = vip.getJSONObject("session_persistence").getString("type");
                if ("SOURCE_IP".equalsIgnoreCase(sessionPersistence)) {
                    persistence = LbPersistence.SUBNET;
                }
                else if (sessionPersistence.endsWith("COOKIE")) {
                    persistence = LbPersistence.COOKIE;
                }
            }
            publicPort = vip.getInt("protocol_port");
            // if we were unable to get the private port from the name, let's see if we can find it from the members
            if( privatePort < 0 ) {
                for( JSONObject member : members ) {
                    if( !member.getString("pool_id").equalsIgnoreCase(lbId) ) {
                        continue;
                    }
                    privatePort = member.getInt("protocol_port");
                    lbListeners.add(LbListener.getInstance(algorithm, persistence, protocol, publicPort, privatePort));
                }
            }
            else {
                lbListeners.add(LbListener.getInstance(algorithm, persistence, protocol, publicPort, privatePort));
            }
        }
        JSONArray monitors = lb.getJSONArray("health_monitors");
        String lbhcId = null;
        if( monitors.length() > 0 ) {
            // Dasein can only support one, should probably extend that in core
            lbhcId = monitors.getString(0);
        }
        LoadBalancer loadBalancer = LoadBalancer.getInstance(
                ownerId, regionId, lbId, state, name, description, LbType.EXTERNAL, addressType, address, lbhcId, VisibleScope.ACCOUNT_REGION, new int[] {publicPort});
        loadBalancer.withProviderSubnetIds(lb.optString("subnet_id"));
        loadBalancer.withListeners(lbListeners.toArray(new LbListener[lbListeners.size()]));
        loadBalancer.operatingIn(regionId + "-a");

        return loadBalancer;
    }

    private LoadBalancerEndpoint toLoadBalancerEnpoint(JSONObject member) throws CloudException {
        try {
            return LoadBalancerEndpoint.getInstance(LbEndpointType.IP,
                    member.getString("address"),
                    "ACTIVE".equalsIgnoreCase(member.getString("status")) ? LbEndpointState.ACTIVE : LbEndpointState.INACTIVE);
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to parse load balancer member" + e.getMessage(), e);


        }
    }

    protected String generateListenerId( String lbId, int privatePort ) {
        return lbId + ":" + privatePort;
    }

    @Override
    public void removeLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.remove");
        try {
            long timeout = System.currentTimeMillis() + ( CalendarWrapper.MINUTE * 5L);
            LoadBalancer lb = getLoadBalancer(loadBalancerId);

            while( LoadBalancerState.PENDING.equals(lb.getCurrentState()) && timeout > System.currentTimeMillis() ) {
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
                try {
                    lb = getLoadBalancer(loadBalancerId);
                    if( lb == null ) {
                        return;
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }

            List<JSONObject> listeners = findAllVips(loadBalancerId);
            for( JSONObject listener : listeners ) {
                try {
                    getMethod().deleteNetworks(getListenersResource(), listener.getString("id"));
                }
                catch( JSONException ignore ) {
                }
            }

            timeout = System.currentTimeMillis() + 15 * CalendarWrapper.MINUTE;

            do {
                try {
                    getMethod().deleteNetworks(getLoadBalancersResource(), loadBalancerId);
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

    @Override
    public LoadBalancer getLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        List<LoadBalancer> loadBalancers = findLoadBalancers(loadBalancerId);
        if( loadBalancers.isEmpty() ) {
            return null;
        }
        return loadBalancers.get(0);
    }

    private String toOSProtocol(LbProtocol networkProtocol) {
        switch( networkProtocol ) {
            case HTTPS:
                return "HTTPS";
            case RAW_TCP:
                return "TCP";
            default:
                return "HTTP";
        }
    }

    protected LoadBalancerHealthCheck.HCProtocol fromOSProtocol(String protocol) {
        if( "HTTPS".equalsIgnoreCase(protocol) ) {
            return LoadBalancerHealthCheck.HCProtocol.HTTPS;
        }
        else if( "TCP".equalsIgnoreCase(protocol) ) {
            return LoadBalancerHealthCheck.HCProtocol.TCP;
        }
        else {
            return LoadBalancerHealthCheck.HCProtocol.HTTP;
        }
    }

    private String toOSAlgorithm(LbAlgorithm algorithm) {
        switch( algorithm ) {
            case LEAST_CONN:
                return "LEAST_CONNECTIONS";
            default:
                return "ROUND_ROBIN";
        }
    }

    // Below is the list of resource endpoints, these will change in LBaaS 2.0
    protected String getLoadBalancersResource() {
        return "v2.0/lb/pools";
    }

    protected String getListenersResource() {
        return "v2.0/lb/vips";
    }

    protected String getMembersResource() {
        return "v2.0/lb/members";
    }

    private String getHealthMonitorsResource() {
        return "v2.0/lb/health_monitors";
    }


}