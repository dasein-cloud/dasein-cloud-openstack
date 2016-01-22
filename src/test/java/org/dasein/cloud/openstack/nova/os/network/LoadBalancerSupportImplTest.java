package org.dasein.cloud.openstack.nova.os.network;

import org.dasein.cloud.Cloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.VisibleScope;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.network.*;
import org.dasein.cloud.openstack.nova.os.AuthenticationContext;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.OpenStackTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.*;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Created by mariapavlova on 01/12/2015.
 */
public class LoadBalancerSupportImplTest extends OpenStackTest {

    private final String testLbId                    = "testLbId";
    private final String testSubnetId                = "testSubnetId";
    private final String testProviderLBHealthCheckId = "testProviderLBHealthCheckId";
    private final String testOwnerId                 = "testOwnerId";
    private final String testRegionId                = "testRegionId";
    private final String testVmId                    = "testVmId";
    private final String testIpAddress1              = "177.177.177.177";
    private final String testIpAddress2              = "188.177.177.177";
    private final int    testPrivatePort             = 9999;

    private LoadBalancer createTestLb() {
        return LoadBalancer.getInstance(testOwnerId, testRegionId, testLbId, LoadBalancerState.ACTIVE, "name", "description", LoadBalancerAddressType.IP, "65.65.65.65", 8080, 8081).withListeners(LbListener.getInstance(8080, testPrivatePort));
    }

    private HealthCheckOptions createHCO() {
        return HealthCheckOptions.getInstance("name", "description", testLbId, null, null, testPrivatePort, null, 1, 1, 1, 1);
    }

    @Test
    public void addIPEndpointsTest() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        LoadBalancer lb = createTestLb();

        try {
            Mockito.doCallRealMethod().when(lbSupport).addIPEndpoints(anyString(), any(String[].class));
            when(lbSupport.getLoadBalancer(anyString())).thenReturn(lb);
            ArgumentCaptor<String> captLbId = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> captAddress = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Integer> captPort = ArgumentCaptor.forClass(Integer.class);

            lbSupport.addIPEndpoints(testLbId, testIpAddress1);

            verify(lbSupport).createMember(captLbId.capture(), captAddress.capture(), captPort.capture());
            assertEquals("Loadbalancer Id is not as expected", testLbId, captLbId.getValue());
            assertEquals("Endpoint Ip address is not as expected", testIpAddress1, captAddress.getValue());
            assertEquals("Endpoint port is not as expected", testPrivatePort, (int) captPort.getValue());
        }
        catch( InternalException | CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void removeServersTest() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        VirtualMachine vmMock = mock(VirtualMachine.class);

        RawAddress[] addresses = new RawAddress[] {
                new RawAddress(testIpAddress1),
                new RawAddress(testIpAddress2)
        };
        try {
            when(vmMock.getPrivateAddresses()).thenReturn(addresses);
            when(lbSupport.getVirtualMachine(anyString())).thenReturn(vmMock);
            Mockito.doCallRealMethod().when(lbSupport).removeServers(anyString(), any(String[].class));

            ArgumentCaptor<String> lbCapt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> address1Capt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> address2Capt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> vmIdCapt = ArgumentCaptor.forClass(String.class);

            lbSupport.removeServers(testLbId, testVmId);

            verify(lbSupport).getVirtualMachine(vmIdCapt.capture());
            assertEquals("Requested vm id is not as expected", testVmId, vmIdCapt.getValue());

            verify(lbSupport).removeIPEndpoints(lbCapt.capture(), address1Capt.capture(), address2Capt.capture());
            assertEquals("Requested lb id is not as expected", testLbId, lbCapt.getValue());
            assertEquals("Requested IP is not as expected", testIpAddress1, address1Capt.getValue());
            assertEquals("Requested IP is not as expected", testIpAddress2, address2Capt.getValue());
        }
        catch( InternalException | CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void createLoadBalancerHealthCheckTest() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        final String testName = "testName";
        final String testDescription = "testDescription";
        final String testHost = "testHost";
        final LoadBalancerHealthCheck.HCProtocol testProtocol = LoadBalancerHealthCheck.HCProtocol.HTTP;
        final int testPort = 42;
        final String testPath = "testPath";
        final int testInterval = 5;
        final int testTimeout = 3;
        final int testHealthyCount = 1;
        final int testUnhealthyCount = 2;
        try {
            when(lbSupport.createLoadBalancerHealthCheck(anyString(), anyString(), anyString(), any(LoadBalancerHealthCheck.HCProtocol.class), anyInt(), anyString(), anyInt(), anyInt(), anyInt(), anyInt())).thenCallRealMethod();
            ArgumentCaptor<HealthCheckOptions> optsCapt = ArgumentCaptor.forClass(HealthCheckOptions.class);
            lbSupport.createLoadBalancerHealthCheck(testName, testDescription, testHost, testProtocol, testPort, testPath, testInterval, testTimeout, testHealthyCount, testUnhealthyCount);
            verify(lbSupport).createLoadBalancerHealthCheck(optsCapt.capture());

            assertEquals("Name is not as expected", testName, optsCapt.getValue().getName());
            assertEquals("Description is not as expected", testDescription, optsCapt.getValue().getDescription());
            assertEquals("Host is not as expected", testHost, optsCapt.getValue().getHost());
            assertEquals("Protocol is not as expected", testProtocol, optsCapt.getValue().getProtocol());
            assertEquals("Port is not as expected", testPort, optsCapt.getValue().getPort());
            assertEquals("Path is not as expected", testPath, optsCapt.getValue().getPath());
            assertEquals("Interval is not as expected", testInterval, optsCapt.getValue().getInterval());
            assertEquals("Timeout is not as expected", testTimeout, optsCapt.getValue().getTimeout());
            assertEquals("HealthyCount is not as expected", testHealthyCount, optsCapt.getValue().getHealthyCount());
            assertEquals("UnhealthyCount is not as expected", testUnhealthyCount, optsCapt.getValue().getUnhealthyCount());
        }
        catch( InternalException | CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void attachHealthCheckToLoadBalancerTest() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        NovaMethod method = mock(NovaMethod.class);
        JSONObject jsonObject = new JSONObject();
        try {
            when(lbSupport.getMethod()).thenReturn(method);
            when(method.postNetworks(anyString(), anyString(), any(JSONObject.class),anyString())).thenReturn(jsonObject);
            Mockito.doCallRealMethod().when(lbSupport).attachHealthCheckToLoadBalancer(anyString(), anyString());

            lbSupport.attachHealthCheckToLoadBalancer(testLbId, testProviderLBHealthCheckId);
            ArgumentCaptor<String> lbCapt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> lbHealthCapt = ArgumentCaptor.forClass(String.class);
            verify(lbSupport).attachHealthCheckToLoadBalancer(lbCapt.capture(), lbHealthCapt.capture());
            assertEquals("Requested lb id is not as expected", testLbId, lbCapt.getValue());
            assertEquals("Requested lb id is not as expected", testProviderLBHealthCheckId, lbHealthCapt.getValue());

        }
        catch( InternalException | CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void listLBHealthChecksTest() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        NovaMethod method = mock(NovaMethod.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/list_health_checks.json");
        LoadBalancer lb = createTestLb();

        try {
            when(lbSupport.listLBHealthChecks(any(HealthCheckFilterOptions.class))).thenCallRealMethod();
            when(lbSupport.getMethod()).thenReturn(method);
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(jsonObject);
            when(lbSupport.toLoadBalancerHealthCheck(any(JSONObject.class))).thenCallRealMethod();
            when(lbSupport.fromOSProtocol(anyString())).thenCallRealMethod();
            when(lbSupport.getLoadBalancer(anyString())).thenReturn(lb);
            Iterable<LoadBalancerHealthCheck> res = lbSupport.listLBHealthChecks(null);
            LoadBalancerHealthCheck testObject = res.iterator().next();
            assertEquals("Returned provider load balancer health check Id does not match", "466c8345-28d8-4f84-a246-e04380b0461d", testObject.getProviderLBHealthCheckId());
            assertEquals("Returned protocol does not match", LoadBalancerHealthCheck.HCProtocol.HTTP, testObject.getProtocol());
            assertEquals("Returned port does not match", 9999, testObject.getPort());
            assertEquals("Returned interval does not match", 10, testObject.getInterval());
            assertEquals("Returned timeout does not match", 1, testObject.getTimeout());
            assertEquals("Returned unhealthyCount does not match", 1, testObject.getUnhealthyCount());
            assertEquals("Returned load balancer Id does not match", "lbId", testObject.getProviderLoadBalancerIds().get(0));
            assertEquals("Returned healthCount does not match", 1, testObject.getHealthyCount());
        }
        catch( InternalException | CloudException | JSONException e ) {
            fail("Test failed " + e.getMessage());
        }
    }
    @Test
    public void getLoadBalancerHealthCheckTest() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        NovaMethod method = mock(NovaMethod.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/get_health_checks.json");
        LoadBalancer lb = createTestLb();
        try {
            when(lbSupport.getMethod()).thenReturn(method);
            when(lbSupport.getLoadBalancerHealthCheck(anyString(), anyString())).thenCallRealMethod();
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(jsonObject);
            when(lbSupport.toLoadBalancerHealthCheck(any(JSONObject.class))).thenCallRealMethod();
            when(lbSupport.fromOSProtocol(anyString())).thenCallRealMethod();
            when(lbSupport.getLoadBalancer(anyString())).thenReturn(lb);
            LoadBalancerHealthCheck res = lbSupport.getLoadBalancerHealthCheck(testProviderLBHealthCheckId, testLbId);
            assertEquals("Returned provider load balancer health check Id does not match", "0a9ac99d-0a09-4b18-8499-a0796850279a", res.getProviderLBHealthCheckId());
            assertEquals("Returned protocol does not match", LoadBalancerHealthCheck.HCProtocol.HTTP, res.getProtocol());
            assertEquals("Returned timeout does not match", 1, res.getTimeout());

        }
        catch( InternalException | CloudException | JSONException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void modifyHealthCheckTest() {
        final HealthCheckOptions testHCO = createHCO();
        final List<String> testLbIds = Arrays.asList(testLbId);

        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        LoadBalancerHealthCheck loadBalancerHealthCheck = mock(LoadBalancerHealthCheck.class);
        when(loadBalancerHealthCheck.getProviderLoadBalancerIds()).thenReturn(testLbIds);

        LoadBalancerHealthCheck anotherLoadBalancerHealthCheck = mock(LoadBalancerHealthCheck.class);
        try {
            when(lbSupport.getLoadBalancerHealthCheck(anyString(), anyString())).thenReturn(loadBalancerHealthCheck);
            when(lbSupport.createHealthMonitor(any(List.class), any(HealthCheckOptions.class))).thenReturn(anotherLoadBalancerHealthCheck);
            when(lbSupport.modifyHealthCheck(anyString(), any(HealthCheckOptions.class))).thenCallRealMethod();

            lbSupport.modifyHealthCheck(testProviderLBHealthCheckId, testHCO);

            ArgumentCaptor<String> lbIdCapt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> lbHcIdCapt = ArgumentCaptor.forClass(String.class);
            verify(lbSupport).getLoadBalancerHealthCheck(lbHcIdCapt.capture(), lbIdCapt.capture());
            assertEquals("Loadbalancer Healthcheck id is not as expected", testProviderLBHealthCheckId, lbHcIdCapt.getValue());
            assertEquals("Loadbalancer id is not as expected", null, lbIdCapt.getValue());

            ArgumentCaptor<LoadBalancerHealthCheck> lbHcCapt = ArgumentCaptor.forClass(LoadBalancerHealthCheck.class);
            verify(lbSupport).safeDeleteLoadBalancerHealthCheck(lbHcCapt.capture());
            assertEquals("Loadbalancer Healthcheck object is not as expected", loadBalancerHealthCheck, lbHcCapt.getValue());

            ArgumentCaptor<List> lbIdsCapt = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<HealthCheckOptions> hcoCapt = ArgumentCaptor.forClass(HealthCheckOptions.class);
            verify(lbSupport).createHealthMonitor(lbIdsCapt.capture(), hcoCapt.capture());

            assertEquals("Loadbalancer Ids are not as expected", testLbIds, lbIdsCapt.getValue());
            assertEquals("HealthCheckOptions object is not as expected", testHCO, hcoCapt.getValue());

        }
        catch( InternalException | CloudException e ) {
            fail("Test failed " + e.getMessage());
        }

    }

    @Test
    public void safeDeleteLoadBalancerHealthCheckTest() {
        final List<String> testLbIds = Arrays.asList(testLbId);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        LoadBalancerHealthCheck loadBalancerHealthCheck = mock(LoadBalancerHealthCheck.class);
        when(loadBalancerHealthCheck.getProviderLoadBalancerIds()).thenReturn(testLbIds);
        when(loadBalancerHealthCheck.getProviderLBHealthCheckId()).thenReturn(testProviderLBHealthCheckId);
        try {
            Mockito.doCallRealMethod().when(lbSupport).safeDeleteLoadBalancerHealthCheck(any(LoadBalancerHealthCheck.class));
            //when(method.deleteNetworks(anyString(),anyString()))

            lbSupport.safeDeleteLoadBalancerHealthCheck(loadBalancerHealthCheck);

            ArgumentCaptor<String> lbhcIdCapt = ArgumentCaptor.forClass(String.class);
            verify(lbSupport).deleteHealthMonitor(lbhcIdCapt.capture());
            assertEquals("Loadbalancer health check Id are not as expected", testProviderLBHealthCheckId, lbhcIdCapt.getValue());

            verify(lbSupport, times(testLbIds.size())).detatchHealthCheck(anyString(), anyString());
            for( String lbId : testLbIds ) {
                ArgumentCaptor<String> lbIdCapt = ArgumentCaptor.forClass(String.class);
                ArgumentCaptor<String> hcIdCapt = ArgumentCaptor.forClass(String.class);
                verify(lbSupport).detatchHealthCheck(lbIdCapt.capture(), hcIdCapt.capture());
                assertEquals("Loadbalancer id is not as expected", lbId, lbIdCapt.getValue());
                assertEquals("Loadbalancer health check id is not as expected", testProviderLBHealthCheckId, hcIdCapt.getValue());
            }

        }
        catch( InternalException | CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void removeLoadBalancerHealthCheckTest() {
        final List<String> testLbIds = Arrays.asList(testLbId);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        LoadBalancerHealthCheck loadBalancerHealthCheck = mock(LoadBalancerHealthCheck.class);
        try {
            when(loadBalancerHealthCheck.getProviderLoadBalancerIds()).thenReturn(testLbIds);
            when(lbSupport.listLBHealthChecks(null)).thenReturn(Arrays.asList(loadBalancerHealthCheck));
            Mockito.doCallRealMethod().when(lbSupport).removeLoadBalancerHealthCheck(anyString());

            lbSupport.removeLoadBalancerHealthCheck(testLbId);

            ArgumentCaptor<LoadBalancerHealthCheck> lbHcCapt = ArgumentCaptor.forClass(LoadBalancerHealthCheck.class);
            verify(lbSupport).safeDeleteLoadBalancerHealthCheck(lbHcCapt.capture());
            assertEquals("Loadbalancer Healthcheck object is not as expected", loadBalancerHealthCheck, lbHcCapt.getValue());
        }
        catch( InternalException | CloudException e ) {
            fail("Test failed " + e.getMessage());
        }

    }

    @Test
    public void createHealthMonitorTest() {
        final List<String> testLbIds = Arrays.asList(testLbId);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        NovaMethod method = mock(NovaMethod.class);
        final HealthCheckOptions testHCO = createHCO();
        final int testTimeout = 1;
        JSONObject jsonObject = readJson("nova/fixtures/lb/create_health_monitor.json");

        try {
            when(lbSupport.getLoadBalancer(anyString())).thenReturn(createTestLb());
            when(lbSupport.toOSHCType(any(LoadBalancerHealthCheck.HCProtocol.class))).thenReturn("HTTP");
            when(lbSupport.getAccountNumber()).thenReturn("testAcc");
            when(lbSupport.getMethod()).thenReturn(method);
            when(method.postNetworks(anyString(), anyString(), any(JSONObject.class), anyString())).thenReturn(jsonObject);
            Mockito.doCallRealMethod().when(lbSupport).createHealthMonitor(anyList(), any(HealthCheckOptions.class));
            when(lbSupport.toLoadBalancerHealthCheck(any(JSONObject.class))).thenCallRealMethod();
            when(lbSupport.fromOSProtocol(anyString())).thenCallRealMethod();
            LoadBalancerHealthCheck res = lbSupport.createHealthMonitor(testLbIds, testHCO);
            ArgumentCaptor<String> lbCapt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> lbHealthCapt = ArgumentCaptor.forClass(String.class);
            verify(lbSupport).attachHealthCheckToLoadBalancer(lbCapt.capture(), lbHealthCapt.capture());
            assertEquals("Returned timeout is not as expected", testTimeout, res.getTimeout());
            assertEquals("Returned provider load balancer health check Id does not match", "0a9ac99d-0a09-4b18-8499-a0796850279a", res.getProviderLBHealthCheckId());
            assertEquals("Returned protocol does not match", LoadBalancerHealthCheck.HCProtocol.HTTP, res.getProtocol());
            assertEquals("Returned port does not match", 9999, res.getPort());
            assertEquals("Returned interval does not match", 1, res.getInterval());
            assertEquals("Returned timeout does not match", 1, res.getTimeout());
            assertEquals("Returned unhealthyCount does not match", 5, res.getUnhealthyCount());
            assertEquals("Returned unhealthyCount does not match", 5, res.getHealthyCount());
        }
        catch( InternalException | CloudException | JSONException e ) {
            fail("Test failed " + e.getMessage());
        }

    }
    @Test
    public void deleteHealthMonitorTest() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        NovaMethod method = mock(NovaMethod.class);

        try {
            Mockito.doCallRealMethod().when(lbSupport).deleteHealthMonitor(anyString());
            when(lbSupport.getMethod()).thenReturn(method);
            lbSupport.deleteHealthMonitor(testProviderLBHealthCheckId);
            verify(method, times(1)).deleteNetworks(anyString(), eq(testProviderLBHealthCheckId));
        }
        catch( CloudException | InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void toLoadBalancerHealthCheck() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/get_health_check.json");
        LoadBalancer lb = mock(LoadBalancer.class);

        try {
            when(lbSupport.toLoadBalancerHealthCheck(any(JSONObject.class))).thenCallRealMethod();
            when(lbSupport.fromOSProtocol(anyString())).thenCallRealMethod();
            when(lbSupport.getLoadBalancer(anyString())).thenReturn(lb);
            when(lb.getListeners()).thenReturn(new LbListener[]{ LbListener.getInstance(8080, 4040) });
            LoadBalancerHealthCheck lbhc = lbSupport.toLoadBalancerHealthCheck(jsonObject.getJSONObject("health_monitor"));
            assertNotNull("The LBHC object should not be null", lbhc);
            assertEquals("The LBHC id is not as expected", "0a9ac99d-0a09-4b18-8499-a0796850279a", lbhc.getProviderLBHealthCheckId());
            assertEquals("The number of attached loadbalancers is not correct", 2, lbhc.getProviderLoadBalancerIds().size());
            assertEquals("The LBHC protocol is not correct", LoadBalancerHealthCheck.HCProtocol.HTTP, lbhc.getProtocol());
            assertEquals("The LBHC interval is not as expected", 2, lbhc.getInterval());
            assertEquals("The LBHC timeout is not as expected", 1, lbhc.getTimeout());
            assertEquals("The LBHC healthy count is not as expected", 5, lbhc.getHealthyCount());
            assertEquals("The LBHC unhealthy count is not as expected", 5, lbhc.getUnhealthyCount());
            assertEquals("The LBHC HTTP path is not as expected", "/index.html", lbhc.getPath());
            assertEquals("The LBHC port is not as expected", 4040, lbhc.getPort());
            assertNull("The LBHC name should be null", lbhc.getName());
            assertNull("The LBHC description should be null", lbhc.getDescription());
            assertNull("The LBHC host should be null", lbhc.getHost());

        } catch (JSONException | InternalException | CloudException e) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void createListener() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        LbListener listener = LbListener.getInstance(80, 8080);
        NovaMethod method = mock(NovaMethod.class);
        try {
            JSONObject result = new JSONObject();
            when(method.postNetworks(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(result);
            Mockito.doCallRealMethod().when(lbSupport).createListener(anyString(), anyString(), any(LbListener.class));
            when(lbSupport.generateListenerId(anyString(), anyInt())).thenCallRealMethod();
            when(lbSupport.getMethod()).thenReturn(method);
            when(lbSupport.getListenersResource()).thenCallRealMethod();
            when(lbSupport.getAccountNumber()).thenReturn(testOwnerId);

            lbSupport.createListener(testLbId, testSubnetId, listener);

            ArgumentCaptor<String> resourceCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceIdCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<JSONObject> bodyCpt = ArgumentCaptor.forClass(JSONObject.class);
            ArgumentCaptor<Boolean> suffixCpt = ArgumentCaptor.forClass(Boolean.class);
            verify(method).postNetworks(resourceCpt.capture(), resourceIdCpt.capture(), bodyCpt.capture(), suffixCpt.capture());
            assertEquals("Resource parameter is wrong", lbSupport.getListenersResource(), resourceCpt.getValue());
            assertNull("Resource Id should be null", resourceIdCpt.getValue());
            JSONObject body = bodyCpt.getValue();
            assertNotNull("Body parameter should not be null", body);
            assertTrue("Body parameter should contain root element 'vip'", body.has("vip"));
            assertTrue("The value of root element 'vip' must implement Map", body.get("vip") instanceof Map);
            Map vip = (Map) body.get("vip");
            assertEquals("Root element 'vip' should contain 6 subelements", 6, vip.size());
            assertEquals("VIP tenant id is not as expected", testOwnerId, vip.get("tenant_id"));
            assertEquals("VIP protocol is not as expected", "TCP", vip.get("protocol"));
            assertEquals("VIP protocol port is not as expected", 80, vip.get("protocol_port"));
            assertEquals("VIP name is not as expected", "testLbId:8080", vip.get("name"));
            assertEquals("VIP subnet id is not as expected", testSubnetId, vip.get("subnet_id"));
            assertEquals("VIP pool id is not as expected", testLbId, vip.get("pool_id"));
            assertEquals("Suffix parameter is wrong", false, suffixCpt.getValue());
        } catch (Exception e) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void createMember() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        NovaMethod method = mock(NovaMethod.class);
        try {
            JSONObject result = new JSONObject();
            when(method.postNetworks(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(result);
            Mockito.doCallRealMethod().when(lbSupport).createMember(anyString(), anyString(), anyInt());
            when(lbSupport.getMethod()).thenReturn(method);
            when(lbSupport.getMembersResource()).thenCallRealMethod();
            when(lbSupport.getAccountNumber()).thenReturn(testOwnerId);

            lbSupport.createMember(testLbId, testIpAddress1, testPrivatePort);

            ArgumentCaptor<String> resourceCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceIdCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<JSONObject> bodyCpt = ArgumentCaptor.forClass(JSONObject.class);
            ArgumentCaptor<Boolean> suffixCpt = ArgumentCaptor.forClass(Boolean.class);
            verify(method).postNetworks(resourceCpt.capture(), resourceIdCpt.capture(), bodyCpt.capture(), suffixCpt.capture());
            assertEquals("Resource parameter is wrong", lbSupport.getMembersResource(), resourceCpt.getValue());
            assertNull("Resource Id should be null", resourceIdCpt.getValue());
            JSONObject body = bodyCpt.getValue();
            assertNotNull("Body parameter should not be null", body);
            assertTrue("Body parameter should contain root element 'member'", body.has("member"));
            assertTrue("The value of root element 'member' must implement Map", body.get("member") instanceof Map);
            Map member = (Map) body.get("member");
            assertEquals("Root element 'member' should contain 4 subelements", 4, member.size());
            assertEquals("Member tenant id is not as expected", testOwnerId, member.get("tenant_id"));
            assertEquals("Member protocol port is not as expected", testPrivatePort, member.get("protocol_port"));
            assertEquals("Member address is not as expected", testIpAddress1, member.get("address"));
            assertEquals("Member pool id is not as expected", testLbId, member.get("pool_id"));
            assertEquals("Suffix parameter is wrong", false, suffixCpt.getValue());
        } catch (Exception e) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void findAllVips() {
        NovaMethod method = mock(NovaMethod.class);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/get_vips.json");
        LoadBalancer lb = createTestLb();
        try {
            when(lbSupport.getMethod()).thenReturn(method);
            when(lbSupport.getAccountNumber()).thenReturn(testOwnerId);
            when(method.getNetworks(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(jsonObject);
            when(lbSupport.findAllVips(anyString())).thenCallRealMethod();

            List<JSONObject> result = lbSupport.findAllVips(testLbId);

            assertNotNull("List of VIPs cannot be null", result);
            ArgumentCaptor<String> resourceCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceIdCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> suffixCpt = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<String> queryCpt = ArgumentCaptor.forClass(String.class);
            verify(method, times(1)).getNetworks(resourceCpt.capture(), resourceIdCpt.capture(), suffixCpt.capture(), queryCpt.capture());
            assertEquals("Resource is not correct", lbSupport.getLoadBalancersResource(), resourceCpt.getValue());
            assertEquals("ResourceId is not correct", null, resourceIdCpt.getValue());
            assertEquals("Suffix is not correct", false, suffixCpt.getValue());
            assertEquals("Query is not correct", "?tenant_id="+ testOwnerId, queryCpt.getValue());

        }
        catch( CloudException e ) {
            e.printStackTrace();
        }
        catch( InternalException e ) {
            e.printStackTrace();
        }

    }

    @Test
    public void findAllMembers() {
        NovaMethod method = mock(NovaMethod.class);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/get_member.json");
        LoadBalancer lb = createTestLb();
        try {
            when(lbSupport.getMethod()).thenReturn(method);
            when(lbSupport.getAccountNumber()).thenReturn(testOwnerId);
            when(method.getNetworks(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(jsonObject);
            when(lbSupport.findAllMembers(anyString())).thenCallRealMethod();

            List<JSONObject> result = lbSupport.findAllMembers(testLbId);

            assertNotNull("List of members cannot be null", result);
            ArgumentCaptor<String> resourceCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceIdCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> suffixCpt = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<String> queryCpt = ArgumentCaptor.forClass(String.class);
            verify(method, times(1)).getNetworks(resourceCpt.capture(), resourceIdCpt.capture(), suffixCpt.capture(), queryCpt.capture());
            assertEquals("Resource is not correct", lbSupport.getLoadBalancersResource(), resourceCpt.getValue());
            assertEquals("ResourceId is not correct", null, resourceIdCpt.getValue());
            assertEquals("Suffix is not correct", false, suffixCpt.getValue());
            assertEquals("Query is not correct", "?tenant_id="+ testOwnerId, queryCpt.getValue());

        }
        catch( CloudException e ) {
            e.printStackTrace();
        }
        catch( InternalException e ) {
            e.printStackTrace();
        }

    }

    @Test
    public void findLoadBalancersWithoutId() {
        NovaMethod method = mock(NovaMethod.class);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/get_pools.json");
        LoadBalancer lb = createTestLb();

        try {
            List<JSONObject> jsonListeners = Arrays.asList(
                    readJson("nova/fixtures/lb/get_vips.json").getJSONArray("vips").getJSONObject(0));
            List<JSONObject> jsonMembers = Arrays.asList(
                    readJson("nova/fixtures/lb/get_member.json").getJSONObject("member"));
            when(lbSupport.getAccountNumber()).thenReturn(testOwnerId);
            when(lbSupport.findAllVips(anyString())).thenReturn(jsonListeners);
            when(lbSupport.findAllMembers(anyString())).thenReturn(jsonMembers);
            when(lbSupport.getMethod()).thenReturn(method);
            when(method.getNetworks(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(jsonObject);
            when(lbSupport.toLoadBalancer(any(JSONObject.class), anyList(), anyList())).thenReturn(lb);
            when(lbSupport.findLoadBalancers(anyString())).thenCallRealMethod();

            List<LoadBalancer> result = lbSupport.findLoadBalancers(null);

            assertNotNull("List of loadbalancers cannot be null", result);
            assertThat("The number of returned loadbalancers is not correct", result.size(), greaterThan(0));
            ArgumentCaptor<String> lbIdCpt = ArgumentCaptor.forClass(String.class);
            verify(lbSupport, times(1)).findAllVips(lbIdCpt.capture());
            assertEquals("LoadbalancerId passed to findAllVips is incorrect", null, lbIdCpt.getValue());

            verify(lbSupport, times(1)).findAllMembers(lbIdCpt.capture());
            assertEquals("LoadbalancerId passed to findAllMembers is incorrect", null, lbIdCpt.getValue());

            ArgumentCaptor<String> resourceCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceIdCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> suffixCpt = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<String> queryCpt = ArgumentCaptor.forClass(String.class);
            verify(method, times(1)).getNetworks(resourceCpt.capture(), resourceIdCpt.capture(), suffixCpt.capture(), queryCpt.capture());
            assertEquals("Resource is not correct", lbSupport.getLoadBalancersResource(), resourceCpt.getValue());
            assertEquals("ResourceId is not correct", null, resourceIdCpt.getValue());
            assertEquals("Suffix is not correct", false, suffixCpt.getValue());
            assertEquals("Query is not correct", "?tenant_id="+ testOwnerId, queryCpt.getValue());


        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( JSONException e ) {
            fail("Test failed " + e.getMessage());
        }


    }

    @Test
    public void findLoadBalancersWithId() {
        NovaMethod method = mock(NovaMethod.class);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/get_pool.json");
        LoadBalancer lb = createTestLb();

        try {
            List<JSONObject> jsonListeners = Arrays.asList(
                    readJson("nova/fixtures/lb/get_vips.json").getJSONArray("vips").getJSONObject(0));
            List<JSONObject> jsonMembers = Arrays.asList(
                    readJson("nova/fixtures/lb/get_member.json").getJSONObject("member"));
            when(lbSupport.getAccountNumber()).thenReturn(testOwnerId);
            when(lbSupport.findAllVips(anyString())).thenReturn(jsonListeners);
            when(lbSupport.findAllMembers(anyString())).thenReturn(jsonMembers);
            when(lbSupport.getMethod()).thenReturn(method);
            when(method.getNetworks(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(jsonObject);
            when(lbSupport.toLoadBalancer(any(JSONObject.class), anyList(), anyList())).thenReturn(lb);
            when(lbSupport.findLoadBalancers(anyString())).thenCallRealMethod();

            List<LoadBalancer> result = lbSupport.findLoadBalancers(testLbId);

            assertNotNull("List of loadbalancers cannot be null", result);
            assertThat("The number of returned loadbalancers is not correct", result.size(), greaterThan(0));
            ArgumentCaptor<String> lbIdCpt = ArgumentCaptor.forClass(String.class);
            verify(lbSupport, times(1)).findAllVips(lbIdCpt.capture());
            assertEquals("LoadbalancerId passed to findAllVips is incorrect", testLbId, lbIdCpt.getValue());

            verify(lbSupport, times(1)).findAllMembers(lbIdCpt.capture());
            assertEquals("LoadbalancerId passed to findAllMembers is incorrect", testLbId, lbIdCpt.getValue());

            ArgumentCaptor<String> resourceCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceIdCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> suffixCpt = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<String> queryCpt = ArgumentCaptor.forClass(String.class);
            verify(method, times(1)).getNetworks(resourceCpt.capture(), resourceIdCpt.capture(), suffixCpt.capture(), queryCpt.capture());
            assertEquals("Resource is not correct", lbSupport.getLoadBalancersResource(), resourceCpt.getValue());
            assertEquals("ResourceId is not correct", testLbId, resourceIdCpt.getValue());
            assertEquals("Suffix is not correct", false, suffixCpt.getValue());
            assertEquals("Query is not correct", "?tenant_id="+ testOwnerId, queryCpt.getValue());


        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( JSONException e ) {
            fail("Test failed " + e.getMessage());
        }


    }

    @Test
    public void toLoadBalancer() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        JSONObject jsonObject = readJson("nova/fixtures/lb/get_pools.json");

        try {
            List<JSONObject> jsonListeners = Arrays.asList(
                    readJson("nova/fixtures/lb/get_vips.json").getJSONArray("vips").getJSONObject(0));

            List<JSONObject> jsonMembers = Arrays.asList(
                    readJson("nova/fixtures/lb/get_member.json").getJSONObject("member"));

            when(lbSupport.toLoadBalancer(any(JSONObject.class), anyList(), anyList())).thenCallRealMethod();
            when(lbSupport.getCurrentRegionId()).thenReturn("testRegion");
            when(lbSupport.findListenerByLbId(anyList(), anyString())).thenCallRealMethod();
            when(lbSupport.findAllVips(anyString())).thenReturn(jsonListeners);
            LoadBalancer lb = lbSupport.toLoadBalancer(
                    jsonObject.getJSONArray("pools").getJSONObject(0),
                    jsonListeners, jsonMembers);
            // let's check everything now
            assertNotNull("Loadbalancer object cannot be null", lb);
            assertEquals("LB address is not as expected", "10.0.0.10", lb.getAddress());
            assertEquals("LB address type is not as expected", LoadBalancerAddressType.IP, lb.getAddressType());
            assertEquals("LB timestamp is not as expected", 0, lb.getCreationTimestamp()); // no information on this so 0 is good
            assertEquals("LB current state is not as expected", LoadBalancerState.ACTIVE, lb.getCurrentState());
            assertEquals("LB description should be empty", "", lb.getDescription());
            assertEquals("LB type is not as expected", LbType.EXTERNAL, lb.getType());
            assertEquals("The number of listeners is wrong", 1, lb.getListeners().length);
            LbListener listener = lb.getListeners()[0];
            assertEquals("Listener algo is not as expected", LbAlgorithm.ROUND_ROBIN, listener.getAlgorithm());
            assertEquals("Listener cookie should be null", null, listener.getCookie());
            assertEquals("Listener network protocol is not as expected", LbProtocol.HTTP, listener.getNetworkProtocol());
            assertEquals("Listener persistence is not as expected", LbPersistence.COOKIE, listener.getPersistence()); // but why cookie is null?
            assertEquals("Listener private port is not as expected", 8080, listener.getPrivatePort());
            assertEquals("Listener public port is not as expected", 80, listener.getPublicPort());
            assertEquals("Listener SSL cert name should be null as they are not supported in Lbaas 1.0",
                    null, listener.getSslCertificateName());
            assertEquals("Listener LBHC id is expected to be null", null, listener.getProviderLBHealthCheckId());
            assertEquals("LB name is not as expected", "app_pool", lb.getName());
            assertEquals("The number of datacenters is wrong", 1, lb.getProviderDataCenterIds().length);
            assertEquals("LB datacenter id is not as expected", "testRegion-a", lb.getProviderDataCenterIds()[0]);
            assertEquals("LB id is not as expected", "72741b06-df4d-4715-b142-276b6bce75ab", lb.getProviderLoadBalancerId());
            assertEquals("LB owner id is not as expected", "83657cfcdfe44cd5920adaf26c48ceea", lb.getProviderOwnerId());
            assertEquals("LB region id is not as expected", "testRegion", lb.getProviderRegionId());
            int subnetCount = 0;
            for( String id : lb.getProviderSubnetIds() ) {
                subnetCount++;
            }
            assertEquals("The number of subnets is wrong", 1, subnetCount);
            assertEquals("LB subnet id is not as expected", "8032909d-47a1-4715-90af-5153ffe39861", lb.getProviderSubnetIds().iterator().next());
            assertEquals("The number of public ports is wrong", 1, lb.getPublicPorts().length);
            assertEquals("LB public port is not as expected", 80, lb.getPublicPorts()[0]);
            assertArrayEquals("LB supported IP version is not as expected", new IPVersion[] { IPVersion.IPV4 }, lb.getSupportedTraffic());
            assertEquals("LB HC id is not as expected", "466c8345-28d8-4f84-a246-e04380b0461d", lb.getProviderLBHealthCheckId());
            assertNull("The firewalls are expected to be null", lb.getProviderFirewallIds()); // FIXME: that's a core bug, must be an empty array/list
            assertEquals("LB visible scope is not as expected", VisibleScope.ACCOUNT_REGION, lb.getVisibleScope());
            assertEquals("LB VLAN id should be null", null, lb.getProviderVlanId());
        } catch( JSONException | InternalException | CloudException e ) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void removeLoadBalancer() {
        NovaMethod method = mock(NovaMethod.class);
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        LoadBalancer lb = createTestLb();
        try {
            List<JSONObject> jsonListeners = Arrays.asList(
                    readJson("nova/fixtures/lb/get_vips.json").getJSONArray("vips").getJSONObject(0));
            when(lbSupport.getLoadBalancer(anyString())).thenReturn(lb);
            Mockito.doCallRealMethod().when(lbSupport).removeLoadBalancer(anyString());
            when(lbSupport.findAllVips(anyString())).thenReturn(jsonListeners);
            when(lbSupport.getMethod()).thenReturn(method);
            lbSupport.removeLoadBalancer(testLbId);
            ArgumentCaptor<String> resourceCpt = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceIdCpt = ArgumentCaptor.forClass(String.class);

            verify(method, times(2)).deleteNetworks(resourceCpt.capture(), resourceIdCpt.capture());
            assertEquals("Resource parameter when deleting a listener was wrong",
                    lbSupport.getListenersResource(), resourceCpt.getAllValues().get(0));
            assertEquals("Resource Id parameter when deleting a listener was wrong",
                    "4ec89087-d057-4e2c-911f-60a3b47ee304", resourceIdCpt.getAllValues().get(0));

            assertEquals("Resource parameter when deleting a loadbalancer was wrong",
                    lbSupport.getLoadBalancersResource(), resourceCpt.getAllValues().get(1));
            assertEquals("Resource Id parameter when deleting a loadbalancer was wrong",
                    testLbId, resourceIdCpt.getAllValues().get(1));


        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());

        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( JSONException e ) {
            fail("Test failed " + e.getMessage());
        }

    }

    @Test
    public void getLoadBalancer() {
        LoadBalancerSupportImpl lbSupport = mock(LoadBalancerSupportImpl.class);
        LoadBalancer lb = createTestLb();
        try {
            when(lbSupport.getLoadBalancer(anyString())).thenCallRealMethod();
            when(lbSupport.findLoadBalancers(anyString())).thenReturn(Collections.singletonList(lb));
            LoadBalancer loadBalancer = lbSupport.getLoadBalancer(testLbId);
            assertEquals("Returned loadbalancer is not correct", lb, loadBalancer);
        }
        catch( CloudException | InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

}

