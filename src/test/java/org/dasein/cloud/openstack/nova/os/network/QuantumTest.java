package org.dasein.cloud.openstack.nova.os.network;

import org.apache.commons.io.IOUtils;
import org.dasein.cloud.*;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.network.*;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.OpenStackTest;
import org.dasein.cloud.test.network.NetworkResources;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Created by mariapavlova on 15/09/2015.
 */
public class QuantumTest extends OpenStackTest {

    @Test
    public void listVlansTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/list_vlans.json");

        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getTenantId()).thenReturn("628b7b037c8a43ef8868327c0accda40");
            when(quantum.getCurrentRegionId()).thenReturn("RegionOne");
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.toVLAN(( JSONObject ) anyObject())).thenCallRealMethod();
            when(quantum.toState(anyString())).thenCallRealMethod();
            when(quantum.listVlans()).thenCallRealMethod();
            Iterable<VLAN> res = quantum.listVlans();
            assertNotNull("Returned list of vlans cannot be null", res);
            int count = 0;
            for( VLAN v : res ) {
                ++count;
            }
            assertEquals("Returned list contains incorrect number of records", 9, count);
            VLAN vlan = res.iterator().next();
            assertEquals("VLAN name does not match", "kstest2", vlan.getName());
            assertEquals("VLAN CIDR does not match", "0.0.0.0/0", vlan.getCidr());
            assertEquals("VLAN state does not match", VLANState.AVAILABLE, vlan.getCurrentState());
            assertEquals("VLAN description does not match", "kstest2", vlan.getDescription());
            assertEquals("VLAN ProviderOwnedId does not match", "628b7b037c8a43ef8868327c0accda40", vlan.getProviderOwnerId());
            assertEquals("VLAN ProviderRegionId does not match", "RegionOne", vlan.getProviderRegionId());
            assertEquals("VLAN ProviderVlanId does not match", "09916d8b-45b5-4059-a928-696d95e4653c", vlan.getProviderVlanId());
            assertEquals("VLAN visible scope does not match", VisibleScope.ACCOUNT_REGION, vlan.getVisibleScope());

        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }

    }

    @Test
    public void listPortsTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        VirtualMachine mv = mock(VirtualMachine.class);

        JSONObject json = readJson("nova/fixtures/list_ports.json");

        when(quantum.getMethod()).thenReturn(method);
        when(mv.getProviderVirtualMachineId()).thenReturn("blah");
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getTenantId()).thenReturn("628b7b037c8a43ef8868327c0accda40");
            when(quantum.getCurrentRegionId()).thenReturn("RegionOne");
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.listPorts(any(VirtualMachine.class))).thenCallRealMethod();

            Iterable<String> res = quantum.listPorts(mv);
            assertNotNull("Returned list of ports cannot be null", res);
            assertEquals("Returned port id does not match", "8c755759-1146-4ca1-a856-fe4867a37689", res.iterator().next());
        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
    }

    @Test
    public void listSubnetsTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        VirtualMachine vm = mock(VirtualMachine.class);
        JSONObject json = readJson("nova/fixtures/list_subnets.json");
        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getTenantId()).thenReturn("e2312698a3534c3aab7038d46a80795d");
            VLAN vlan = new VLAN();
            vlan.setProviderVlanId("5761fd9c-30b9-4064-a42d-1b181decaa8e");
            vlan.setProviderOwnerId("e2312698a3534c3aab7038d46a80795d");
            vlan.setProviderRegionId("RegionOne");
            when(quantum.getVlan(anyString())).thenReturn(vlan);
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.listSubnets(anyString())).thenCallRealMethod();
            when(quantum.toSubnet(( JSONObject ) anyObject(), ( VLAN ) anyObject())).thenCallRealMethod();

            Iterable<Subnet> res = quantum.listSubnets("5761fd9c-30b9-4064-a42d-1b181decaa8e");
            assertNotNull("Returned list of subnets cannot be null", res);
            int count = 0;
            for( Subnet v : res ) {
                ++count;
            }
            assertEquals("Returned list contains incorrect number of records", 8, count);
            Subnet subnet = res.iterator().next();
            assertEquals("VLAN name does not match", "dsnnet7014-subnet", subnet.getName());
            assertEquals("VLAN state does not match", SubnetState.AVAILABLE, subnet.getCurrentState());

        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }

    }

    @Test
    public void getVlanTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/get_vlan.json");
        try {
            when(quantum.getMethod()).thenReturn(method);
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getVlan(anyString())).thenCallRealMethod();
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.toState(anyString())).thenCallRealMethod();
            when(quantum.toVLAN(( JSONObject ) anyObject())).thenCallRealMethod();
            VLAN res = quantum.getVlan("5761fd9c-30b9-4064-a42d-1b181decaa8e");
            assertNotNull("Returned list of vlans cannot be null", res);
            assertEquals("VLAN state does not match", VLANState.AVAILABLE, res.getCurrentState());

        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
    }

    @Test
    public void getSubnetTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/get_subnet.json");
        try {
            when(quantum.getMethod()).thenReturn(method);
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getSubnet(anyString())).thenCallRealMethod();
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.toSubnet(( JSONObject ) anyObject(), ( VLAN ) anyObject())).thenCallRealMethod();
            VLAN vlan = new VLAN();
            vlan.setProviderVlanId("5761fd9c-30b9-4064-a42d-1b181decaa8e");
            vlan.setProviderOwnerId("e2312698a3534c3aab7038d46a80795d");
            vlan.setProviderRegionId("RegionOne");
            when(quantum.getVlan(anyString())).thenReturn(vlan);
            Subnet res = quantum.getSubnet("66435044-1513-4f23-9f65-5557b130a008");
            assertNotNull("Returned list of subnets cannot be null", res);

        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
    }

    @Test
    public void createPortTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/create_port.json");
        Subnet subnetStub = Subnet.getInstance("testOwnerId", "testRegionId", "testVlanId", "testSubnetId", SubnetState.AVAILABLE, "testSubnetName", "testSubnetDescription", "testSubnetCidr");
        try {
            JSONObject fakeResponse = new JSONObject().put("port", new JSONObject().put("id", "testPortId"));

            when(quantum.getMethod()).thenReturn(method);
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getSubnet(anyString())).thenReturn(subnetStub);
            when(method.postNetworks(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(fakeResponse);

            when(quantum.createPort(anyString(), anyString(), any(String[].class))).thenCallRealMethod();
            ArgumentCaptor<JSONObject> argument = ArgumentCaptor.forClass(JSONObject.class);

            String res = quantum.createPort("testSubnetId", "testVmName", null);
            verify(method).postNetworks(anyString(), anyString(), argument.capture(), anyBoolean());

            assertNotNull("Returned port id cannot be null", res);
            assertEquals("Returned port id is not as expected", "testPortId", res);
            // need to compare jsons as strings as JSONObject doesn't seem to implement equals() method
            assertEquals("JSON argument did not match", json.toString(), argument.getValue().toString());
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( JSONException e ) {
            throw new RuntimeException("Unable to construct a fake response object");
        }

    }

    @Test
    public void createSubnetTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        VLANCapabilities capabilities = mock(VLANCapabilities.class);
        JSONObject json = readJson("nova/fixtures/create_subnet.json");
        VLAN vlan = new VLAN();
        vlan.setProviderVlanId("testVlanId");
        vlan.setProviderOwnerId("testTenantId");
        vlan.setProviderRegionId("RegionOne");
        vlan.setCidr("testSubnetCidr");
        vlan.setName("Port for testVmName");
        SubnetCreateOptions subnetOptions = SubnetCreateOptions.getInstance("testVlanId", "testSubnetCidr", "testSubnetName", "testSubnetDescription");
        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(capabilities.allowsNewSubnetCreation()).thenReturn(true);
            when(quantum.getCapabilities()).thenReturn(capabilities);
            when(quantum.getVlan(anyString())).thenReturn(vlan);
            when(method.postNetworks(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(json);
            when(quantum.createSubnet(subnetOptions)).thenCallRealMethod();
            when(quantum.toSubnet(( JSONObject ) anyObject(), ( VLAN ) anyObject())).thenCallRealMethod();
            Subnet res = quantum.createSubnet(subnetOptions);
            ArgumentCaptor<JSONObject> argument = ArgumentCaptor.forClass(JSONObject.class);
            verify(method).postNetworks(anyString(), anyString(), argument.capture(), anyBoolean());

            JSONObject subnetObj = new JSONObject();
            subnetObj.put("network_id", "testVlanId");
            subnetObj.put("name", "testSubnetName");
            subnetObj.put("ip_version", "4");
            subnetObj.put("cidr", "testSubnetCidr");
            JSONObject requestVerification = new JSONObject().put("subnet", subnetObj);

            assertEquals("Requested JSON is not as expected", requestVerification.toString(), argument.getValue().toString());

            assertNotNull("Returned subnet id cannot be null", res);
            assertEquals("Returned subnet id is not as expected", "testSubnetId", res.getProviderSubnetId());
            assertEquals("Returned VLAN id is not as expected", "testVlanId", res.getProviderVlanId());
            assertEquals("Returned cidr is not as expected", "testSubnetCidr", res.getCidr());
            assertEquals("Returned name is not as expected", "testVmName", res.getName());
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
    public void createVlanTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/create_vlan.json");
        VLAN vlan = new VLAN();
        vlan.setProviderVlanId("testVlanId");
        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.NOVA);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(json);
            when(quantum.createVlan(anyString(), anyString(), anyString(), anyString(), any(String[].class), any(String[].class))).thenCallRealMethod();
            when(quantum.toVLAN(( JSONObject ) anyObject())).thenCallRealMethod();
            VLAN res = quantum.createVlan("testSubnetCidr", "name", "description", "domainName", new String[]{"dnsServers"}, new String[]{"ntpServers"});
            ArgumentCaptor<JSONObject> argument = ArgumentCaptor.forClass(JSONObject.class);
            verify(method).postServers(anyString(), anyString(), argument.capture(), anyBoolean());
            JSONObject vlanObj = new JSONObject();
            vlanObj.put("label", "name");
            vlanObj.put("cidr", "testSubnetCidr");
            JSONObject metadataObj = new JSONObject().put("org.dasein.ntp.1", "ntpServers").put("org.dasein.domain", "domainName").put("org.dasein.description", "description").put("org.dasein.dns.1", "dnsServers");
            vlanObj.put("metadata", metadataObj);
            JSONObject requestVerification = new JSONObject().put("network", vlanObj);

            assertEquals("Requested JSON is not as expected", requestVerification.toString(), argument.getValue().toString());
            assertNotNull("Returned vlan cannot be null", res);
            assertEquals("Returned vlan id is not as expected", "testVlanId", res.getProviderVlanId());
            assertEquals("Returned cidr id is not as expected", "testSubnetCidr", res.getCidr());
            assertEquals("Returned name is not as expected", "testVlanName", res.getName());
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
    public void getCapabilitiesTest() {
        NetworkCapabilities capabilities = mock(NetworkCapabilities.class);
        Quantum quantum = mock(Quantum.class);
        try {
            when(quantum.getCapabilities()).thenReturn(capabilities);
            NetworkCapabilities res = ( NetworkCapabilities ) quantum.getCapabilities();
            assertNotNull("Returned capabilities cannot be null", res);
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getNetworkResourceTest() {
        Quantum.QuantumType type = Quantum.QuantumType.QUANTUM;
        Quantum quantum = mock(Quantum.class);
        try {
            when(quantum.getNetworkType()).thenReturn(type);
            String res = type.getNetworkResource();
            assertNotNull("Returned network resource cannot be null", res);
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getNetworkResourceVersionTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/get_networkResourceVersion.json");
        try {
            when(quantum.getMethod()).thenReturn(method);
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.getNetworkResourceVersion()).thenCallRealMethod();
            String res = quantum.getNetworkResourceVersion();
            assertNotNull("Returned list of network resource versions cannot be null", res);
            //assertEquals("Version status does not match", Ver);

        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }

    }

    @Test
    public void getPortResourceTest() {
        Quantum.QuantumType type = Quantum.QuantumType.QUANTUM;
        Quantum quantum = mock(Quantum.class);
        try {
            when(quantum.getNetworkType()).thenReturn(type);
            String res = type.getPortResource();
            assertNotNull("Returned network resource cannot be null", res);
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getSubnetResource() {
        Quantum.QuantumType type = Quantum.QuantumType.QUANTUM;
        Quantum quantum = mock(Quantum.class);
        try {
            when(quantum.getNetworkType()).thenReturn(type);
            String res = type.getSubnetResource();
            assertNotNull("Returned network resource cannot be null", res);
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void isSubscribedTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/get_vlan.json");
        try {
            when(quantum.getMethod()).thenReturn(method);
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.isSubscribed()).thenCallRealMethod();
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            Boolean res = quantum.isSubscribed();
            assertFalse("Cannot be true", res);
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void listPortsByNetworkIdTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/list_ports.json");
        final String testVlanId = "testVlanId";
        VLAN vlan = new VLAN();
        vlan.setProviderVlanId(testVlanId);

        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getTenantId()).thenReturn("628b7b037c8a43ef8868327c0accda40");
            when(quantum.getCurrentRegionId()).thenReturn("RegionOne");
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.listPortsByNetworkId(testVlanId)).thenCallRealMethod();

            Iterable<String> res = quantum.listPortsByNetworkId(testVlanId);
            ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
            verify(method).getNetworks(argument.capture(), anyString(), anyBoolean());
            assertTrue("Network request does not contain network_id parameter", argument.getValue().contains("network_id=" + testVlanId));
            assertNotNull("Returned list of ports cannot be null", res);
            assertEquals("Returned port id does not match", "8c755759-1146-4ca1-a856-fe4867a37689", res.iterator().next());
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void listPortsBySubnetId() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        Subnet subnet = mock(Subnet.class);
        JSONObject json = readJson("nova/fixtures/list_ports.json");
        final String testSubnetId = "66435044-1513-4f23-9f65-5557b130a008";
        when(quantum.getMethod()).thenReturn(method);
        when(subnet.getProviderVlanId()).thenReturn(testSubnetId);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getSubnet(testSubnetId)).thenReturn(subnet);
            when(quantum.getTenantId()).thenReturn("628b7b037c8a43ef8868327c0accda40");
            when(quantum.getCurrentRegionId()).thenReturn("RegionOne");
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.listPortsBySubnetId(testSubnetId)).thenCallRealMethod();

            Iterable<String> res = quantum.listPortsBySubnetId(testSubnetId);
            assertNotNull("Returned list of ports cannot be null", res);
            assertEquals("Returned port id does not match", "8c755759-1146-4ca1-a856-fe4867a37689", res.iterator().next());
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }

    }

    @Test
    public void listVlanStatusTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/list_vlans.json");
        final String testProviderResourceId = "09916d8b-45b5-4059-a928-696d95e4653c";
        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getTenantId()).thenReturn("628b7b037c8a43ef8868327c0accda40");
            when(quantum.getCurrentRegionId()).thenReturn("RegionOne");
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(quantum.toStatus(( JSONObject ) anyObject())).thenCallRealMethod();
            when(quantum.listVlanStatus()).thenCallRealMethod();
            Iterable<ResourceStatus> res = quantum.listVlanStatus();
            assertNotNull("Returned vlan status cannot be null", res);
            int count = 0;
            for( ResourceStatus rs : res ) {
                ++count;
            }
            assertEquals("Returned list contains incorrect number of records", 9, count);
            ResourceStatus rs = res.iterator().next();
            assertEquals("VLAN ProviderResourceId does not match", "09916d8b-45b5-4059-a928-696d95e4653c", rs.getProviderResourceId());
        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }

    }

    @Test
    public void listResourcesTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        final String testInVlanId = "testInVlanId";

        ComputeServices services = mock(ComputeServices.class);
        VirtualMachineSupport virtualMachineSupport = mock(VirtualMachineSupport.class);
        VirtualMachine virtualMachine1 = mock(VirtualMachine.class);
        when(virtualMachine1.getProviderVlanId()).thenReturn(testInVlanId);
        VirtualMachine virtualMachine2 = mock(VirtualMachine.class);
        when(quantum.getMethod()).thenReturn(method);
        when(services.getVirtualMachineSupport()).thenReturn(virtualMachineSupport);
        try {
            when(quantum.getServices()).thenReturn(services);
            when(virtualMachineSupport.listVirtualMachines()).thenReturn(Arrays.asList(virtualMachine1, virtualMachine2));
            when(quantum.toStatus(( JSONObject ) anyObject())).thenCallRealMethod();
            when(quantum.listResources(anyString())).thenCallRealMethod();
            Iterable<Networkable> res = quantum.listResources(testInVlanId);
            assertNotNull("Returned list of resources cannot be null", res);

            int count = 0;
            for( Networkable networkable : res ) {
                count++;
            }
            assertEquals("Returned number of resources is not as expected", 1, count);
            assertEquals("Returned resource is not as expected", virtualMachine1, res.iterator().next());
        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }

    }
    @Test
    public void removeVlanTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        final String portId = "testPortId";
        final String testVlanId = "testVlanId";

        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.listPortsByNetworkId(anyString())).thenReturn(Arrays.asList(portId));
            Mockito.doCallRealMethod().when(quantum).removeVlan(anyString());

            quantum.removeVlan(testVlanId);

            verify(quantum, atLeast(1)).getNetworkType();
            verify(quantum, times(1)).listPortsByNetworkId(testVlanId);
            verify(quantum, times(1)).removePort(portId);
            verify(method, times(1)).deleteNetworks(anyString(), eq(testVlanId));
        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
    }
    @Test
    public void removeSubnetTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        final String portId = "testPortId";
        final String testSubnetId = "testSubnetId";

        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.listPortsBySubnetId(anyString())).thenReturn(Arrays.asList(portId));
            Mockito.doCallRealMethod().when(quantum).removeSubnet(anyString());

            quantum.removeSubnet(testSubnetId);

            verify(quantum, atLeast(1)).getNetworkType();
            verify(quantum, times(1)).listPortsBySubnetId(testSubnetId);
            verify(quantum, times(1)).removePort(portId);
            verify(method, times(1)).deleteNetworks(anyString(), eq(testSubnetId));
        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
    }
    @Test
    public void removePortTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        final String testPortId = "testPortId";

        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            Mockito.doCallRealMethod().when(quantum).removePort(anyString());

            quantum.removePort(testPortId);
            verify(quantum, atLeast(1)).getNetworkType();
            verify(method, times(1)).deleteNetworks(anyString(), eq(testPortId + ".json"));

        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
    }
    @Test
    public void toStateTest() {
        Quantum quantum = mock(Quantum.class);
            when(quantum.toState(anyString())).thenCallRealMethod();
            assertEquals("State not as expected", VLANState.AVAILABLE, quantum.toState("active"));
            assertEquals("State not as expected", VLANState.PENDING, quantum.toState("build"));
            assertEquals("State not as expected", VLANState.PENDING, quantum.toState("blah"));
    }
    @Test
    public void toStatusTest() {
        Quantum quantum = mock(Quantum.class);
        JSONObject jsonObject = new JSONObject();
        final String testId = "testId";

        try {
            when(quantum.toStatus(( JSONObject ) anyObject())).thenCallRealMethod();
            when(quantum.toState(anyString())).thenCallRealMethod();
            assertNull("Return value not as expected", quantum.toStatus(jsonObject));

            jsonObject.put("id", testId);
            ResourceStatus resourceStatus = quantum.toStatus(jsonObject);
            assertEquals("Status resource id not as expected", testId, resourceStatus.getProviderResourceId());
            assertEquals("State not as expected", VLANState.AVAILABLE, resourceStatus.getResourceStatus());

            jsonObject.put("status", "build");
            resourceStatus = quantum.toStatus(jsonObject);
            assertEquals("State not as expected", VLANState.PENDING, resourceStatus.getResourceStatus());
            verify(quantum, times(1)).toState(anyString());
        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
        catch( JSONException e ) {
            throw new RuntimeException("Error while handling JSON", e);
        }
    }

    @Test
    public void toSubnetTest() {
        Quantum quantum = mock(Quantum.class);
        JSONObject jsonObject = new JSONObject();
        VLAN vlan = new VLAN();
        vlan.setCidr("testCidr");
        vlan.setName("testName");
        vlan.setDescription("testDescription");
        final String networkId = "testId";
        final String testSubnetId = "testSubnetId";

        try {
            when(quantum.toSubnet(( JSONObject ) anyObject(), ( VLAN ) anyObject())).thenCallRealMethod();
            when(quantum.getVlan(anyString())).thenReturn(vlan);
            assertNull(quantum.toSubnet(jsonObject, null));
            jsonObject.put("network_id", networkId);
            quantum.toSubnet(jsonObject, null);
            verify(quantum, times(1)).getVlan(networkId);

            when(quantum.getVlan(anyString())).thenReturn(null);
            assertNull(quantum.toSubnet(jsonObject, null));

            jsonObject.put("id", testSubnetId);
            jsonObject.put("cidr", "testSubnetCidr");
            jsonObject.put("name", "testName");
            jsonObject.put("description", "testDescription");
            JSONObject metadataObj = new JSONObject().put("org.dasein.description", "description").put("org.dasein.name", "domainName");
            jsonObject.put("metadata", metadataObj);
            jsonObject.put("ip_version", "4");
            JSONArray allocPoolsArr = new JSONArray();
            jsonObject.put("allocation_pools", allocPoolsArr);
            Subnet res = quantum.toSubnet(jsonObject, vlan);
            assertEquals("Returned name is not as expected ", "testName", res.getName());
            assertEquals("Returned cidr is not as expected ", "testSubnetCidr", res.getCidr());


        }
        catch( CloudException e ) {
            fail("Unexpected exception " + e);
        }
        catch( InternalException e ) {
            fail("Unexpected exception " + e);
        }
        catch( JSONException e ) {
            throw new RuntimeException("Error while handling JSON", e);
        }
    }

    @Test
    public void toVlanTest() {
        Quantum quantum = mock(Quantum.class);
        JSONObject jsonObject = new JSONObject();
        VLAN vlan = new VLAN();
        vlan.setCidr("testCidr");
        vlan.setName("testName");
        vlan.setDescription("testDescription");
        final String networkId = "testId";
        final String testSubnetId = "testSubnetId";
        // TODO
    }

}
