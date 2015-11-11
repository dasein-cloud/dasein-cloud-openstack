package org.dasein.cloud.openstack.nova.os.compute;

import org.apache.commons.io.IOUtils;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.network.*;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.OpenStackProvider;
import org.dasein.cloud.openstack.nova.os.network.NovaFloatingIP;
import org.dasein.cloud.openstack.nova.os.network.NovaNetworkServices;
import org.dasein.cloud.openstack.nova.os.network.NovaSecurityGroup;
import org.dasein.cloud.openstack.nova.os.network.Quantum;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Created by mariapavlova on 09/10/2015.
 */
public class NovaServerTest {
    private JSONObject readJson(String filename) {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(filename);
            String jsonText = IOUtils.toString(is);
            return new JSONObject(jsonText);
        }
        catch( JSONException e ) {
            throw new RuntimeException(e);
        }
        catch( IOException e ) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void getConsoleOutputTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);

        final String testVmId = "testVmId";
        try {
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getConsoleOutput(testVmId)).thenCallRealMethod();
            when(server.getMethod()).thenReturn(method);
            when(method.postServersForString(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn("blah");

            ArgumentCaptor<JSONObject> jsonArg = ArgumentCaptor.forClass(JSONObject.class);
            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            assertEquals("Console output is not as expected", "blah", server.getConsoleOutput(testVmId));

            verify(method).postServersForString(anyString(), vmIdArg.capture(), jsonArg.capture(), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testVmId, vmIdArg.getValue());
            JSONObject serverObj = new JSONObject();
            serverObj.put("os-getConsoleOutput", new HashMap<String, Object>());
            assertEquals("JSON object passed to the method is not as expected", serverObj.toString(), jsonArg.getValue().toString());

        }
        catch( JSONException | InternalException | CloudException e) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getProductTest() {
        NovaServer novaServer = mock(NovaServer.class);
        try {
            List<NovaServer.FlavorRef> listOfFlavors = new ArrayList<NovaServer.FlavorRef>();
            listOfFlavors.add(new NovaServer.FlavorRef());
            when(novaServer.listFlavors()).thenReturn(listOfFlavors);
        }
        catch( InternalException | CloudException e) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getServerStatusTest(){
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);

        final String testVirtualMachineId = "testVirtualMachineId";
        final String testStatus = "testStatus";

        try {
            JSONObject json = new JSONObject();
            JSONObject serverJson = new JSONObject();
            serverJson.put("status", testStatus);
            json.put("server", serverJson);

            when(server.getMethod()).thenReturn(method);
            when(server.getServerStatus(testVirtualMachineId)).thenCallRealMethod();
            ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
            when(method.getServers(anyString(), argument.capture(), anyBoolean())).thenReturn(json);
            String status = server.getServerStatus(testVirtualMachineId);
            verify(method).getServers(anyString(), anyString(), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testVirtualMachineId, argument.getValue());
            assertEquals("Return value is not as expected", testStatus, status);
        }
        catch( JSONException | InternalException | CloudException e) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getVirtualMachineTest(){
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        NovaNetworkServices networkServicesMock = mock(NovaNetworkServices.class);
        NovaFloatingIP ipAddressSupportMock = mock(NovaFloatingIP.class);
        Quantum vlanSupportMock = mock(Quantum.class);

        JSONObject json = readJson("nova/fixtures/compute/get_server.json");
        final String testVmId = "testVmId";
        try {
            when(server.getNetworkServices()).thenReturn(networkServicesMock);
            when(networkServicesMock.getIpAddressSupport()).thenReturn(ipAddressSupportMock);
            when(ipAddressSupportMock.listIpPool(any(IPVersion.class), anyBoolean())).thenReturn(Collections.EMPTY_LIST);
            when(networkServicesMock.getVlanSupport()).thenReturn(vlanSupportMock);
            when(vlanSupportMock.listVlans()).thenReturn(Collections.EMPTY_LIST);

            when(server.getMethod()).thenReturn(method);
            when(server.getTenantId()).thenReturn("openstack");
            when(server.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
            when(server.getMinorVersion()).thenReturn(1);
            when(server.getMajorVersion()).thenReturn(2);
            when(server.getPlatform(anyString(), anyString(), anyString())).thenReturn(Platform.UBUNTU);

            when(server.getRegionId()).thenReturn("testRegion");
            when(server.listFirewalls(anyString())).thenReturn(Collections.EMPTY_LIST);
            when(server.toVirtualMachine(any(JSONObject.class), anyList(), anyList(), anyList())).thenCallRealMethod();
            when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(server.getVirtualMachine(anyString())).thenCallRealMethod();
            VirtualMachine vm = server.getVirtualMachine(testVmId);
            assertEquals("VM ID is not as expected ", "7838ff1b-b71f-48b9-91e9-7c08de20b249", vm.getProviderVirtualMachineId());
            assertEquals("Current State is not as expected ", VmState.RUNNING, vm.getCurrentState());
            assertEquals("Description is not as expected ", "new-server-test", vm.getDescription());
            assertEquals("VM name is not as expected ", "new-server-test", vm.getName());
            assertEquals("Provider Data Center ID is not as expected ", "testRegion-a", vm.getProviderDataCenterId());
            assertEquals("Provider Machine Image ID is not as expected ", "70a599e0-31e7-49b7-b260-868f441e862b", vm.getProviderMachineImageId());
            assertEquals("Provider Owner ID is not as expected ", "openstack", vm.getProviderOwnerId());
            assertEquals("Provider Region ID is not as expected ", "testRegion", vm.getProviderRegionId());

        }
        catch( JSONException | InternalException | CloudException e) {
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void launchTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);

        JSONObject jsonResponse = mock(JSONObject.class);
        when(jsonResponse.has(anyString())).thenReturn(true);

        JSONObject jsonRequest = readJson("nova/fixtures/compute/create_server.json");

        final String testProductId = "productId";
        final String testMachineImageId = "70a599e0-31e7-49b7-b260-868f441e862b";
        final String testVmName = "machineName";
        final String testVmDescription = "machineDescription";
        final String testMachineImageRef = "machineImageRef";
        final String testFlavorRef = "flavorRef";
        final String testVmId = "7838ff1b-b71f-48b9-91e9-7c08de20b249";
        final VirtualMachine vm = mock(VirtualMachine.class);

        MachineImage machineImageMock = mock(MachineImage.class);
        when(machineImageMock.getTag("minSize")).thenReturn("10");
        when(machineImageMock.getPlatform()).thenReturn(Platform.UBUNTU);

        VMLaunchOptions options = VMLaunchOptions.getInstance(testProductId, testMachineImageId, testVmName, testVmDescription);

        try {
            when(server.launch(any(VMLaunchOptions.class))).thenCallRealMethod();
            when(server.getImage(testMachineImageId)).thenReturn(machineImageMock);
            when(server.getImageRef(options)).thenReturn(testMachineImageRef);
            when(server.getFlavorRef(anyString())).thenReturn(testFlavorRef);
            when(server.isBareMetal(anyString())).thenReturn(false);
            when(server.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
            when(server.getMethod()).thenReturn(method);

            ArgumentCaptor<JSONObject> jsonArg = ArgumentCaptor.forClass(JSONObject.class);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(jsonResponse);

            when(server.toVirtualMachine(any(JSONObject.class), anyList(), anyList(), anyList())).thenReturn(vm);
            when(vm.getProviderVirtualMachineId()).thenReturn(testVmId);
            when(vm.getCurrentState()).thenReturn(VmState.RUNNING);

            // Run the method under test
            server.launch(options);

            // Verify
            verify(method).postServers(anyString(), anyString(), jsonArg.capture(), anyBoolean());

            // Convert captured value to JSONObject via String as the actual value being sent is made up of hashmaps,
            // they don't compare so well with proper JSONObject's.
            JSONAssert.assertEquals(jsonRequest, new JSONObject(jsonArg.getValue().toString()), false);
        }
        catch( JSONException | InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void listFirewallsTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaSecurityGroup securityGroups = mock(NovaSecurityGroup.class);
        NovaServer server = mock(NovaServer.class);
        Firewall fakeFirewall = new Firewall();
        fakeFirewall.setName("default");
        fakeFirewall.setProviderFirewallId("default");
        List<Firewall> fakeFirewalls = Arrays.asList(fakeFirewall);

        JSONObject json = readJson("nova/fixtures/compute/get_server.json");
        final String testVmId = "7838ff1b-b71f-48b9-91e9-7c08de20b249";
        try {
            when(server.getMethod()).thenReturn(method);
            when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(server.listFirewalls(anyString())).thenCallRealMethod();
            when(server.listFirewalls(anyString(), any(JSONObject.class))).thenCallRealMethod();
            when(server.getNovaSecurityGroup()).thenReturn(securityGroups);
            when(securityGroups.list()).thenReturn(fakeFirewalls);

            Iterable<String> res = server.listFirewalls(testVmId);
            assertNotNull("Returned list of firewalls cannot be null", res);
            assertEquals("Returned firewall is not the one we expected", fakeFirewall.getProviderFirewallId(), res.iterator().next());

        }
        catch( CloudException e ) {
            e.printStackTrace();
        }
        catch( InternalException e ) {
            e.printStackTrace();
        }

    }

    @Test
    public void listFirewallTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaSecurityGroup securityGroups = mock(NovaSecurityGroup.class);
        NovaServer server = mock(NovaServer.class);
        Firewall firewall = mock(Firewall.class);
        Firewall fakeFirewall = new Firewall();
        fakeFirewall.setName("default");
        fakeFirewall.setProviderFirewallId("default");
        List<Firewall> fakeFirewalls = Arrays.asList(fakeFirewall);

        JSONObject json = readJson("nova/fixtures/compute/get_server.json");
        final String testProviderFirewallId = "testProviderFirewallId";
        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getNovaSecurityGroup()).thenCallRealMethod();
            when(firewall.getProviderFirewallId()).thenReturn(testProviderFirewallId);
            when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(server.listFirewalls(anyString(), any(JSONObject.class))).thenCallRealMethod();

        }
        catch( CloudException e ) {
            e.printStackTrace();
        }
        catch( InternalException e ) {
            e.printStackTrace();
        }
    }

        @Test
        public void listFlavorsTest() {
            NovaMethod method = mock(NovaMethod.class);
            NovaServer server = mock(NovaServer.class);
            JSONObject json = readJson("nova/fixtures/compute/list_flavors.json");

            try {
                when(server.getMethod()).thenReturn(method);
                when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
                when(server.toProduct(any(JSONObject.class))).thenCallRealMethod();
                when(server.listFlavors()).thenCallRealMethod();
                Iterable<NovaServer.FlavorRef> flavorRefs = server.listFlavors();
                assertNotNull("List of flavours cannot be null", flavorRefs);
                int count = 0;
                for( NovaServer.FlavorRef ref: flavorRefs) {
                   count++;
                }
                assertEquals("Flavor count is incorrect", 5, count);
                NovaServer.FlavorRef ref = flavorRefs.iterator().next();
                assertEquals("Flavor id not correct", "1", ref.id);
                assertNotNull("Flavor ref cannot be null", ref.product);
                assertEquals("Flavor name is incorrect", "m1.tiny", ref.product.getName());
                assertEquals("Flavor product id is incorrect", "1", ref.product.getProviderProductId());
            }
            catch( JSONException | InternalException | CloudException e) {
                e.printStackTrace();
                fail("Test failed " + e.getMessage());
            }
        }
    @Test
    public void listProductsTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        JSONObject json = readJson("nova/fixtures/compute/list_flavors.json");
        VirtualMachineProductFilterOptions productFilterOptions = VirtualMachineProductFilterOptions.getInstance();

        try {
            NovaServer.FlavorRef ref = new NovaServer.FlavorRef();
            ref.id = "1";
            ref.product = new VirtualMachineProduct();
            ref.product.setName("test");
            ref.product.setProviderProductId("1");
            Iterable<NovaServer.FlavorRef> flavorRefs = Collections.singleton(ref);
            when(server.listFlavors()).thenReturn(flavorRefs);
            when(server.listProducts(anyString(), any(VirtualMachineProductFilterOptions.class))).thenCallRealMethod();
            Iterable<VirtualMachineProduct> products = server.listProducts("1", productFilterOptions);
            assertNotNull("List of products cannot be null", products);
            int count = 0;
            for( VirtualMachineProduct prod: products) {
                count++;
            }
            assertEquals("Product count is incorrect", 1, count);

        }
        catch(  InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }
    @Test
    public void listVirtualMachinesTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        NovaNetworkServices networkServicesMock = mock(NovaNetworkServices.class);
        NovaFloatingIP ipAddressSupportMock = mock(NovaFloatingIP.class);
        Quantum vlanSupportMock = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/compute/list_servers.json");
        try {
            when(server.getMethod()).thenReturn(method);
            //when(networkServicesMock.getIpAddressSupport()).thenReturn(ipAddressSupportMock);
            when(ipAddressSupportMock.listIpPool(any(IPVersion.class), anyBoolean())).thenReturn(Collections.EMPTY_LIST);
            when(server.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
            when(server.listFirewalls(anyString(), any(JSONObject.class))).thenReturn(Collections.EMPTY_LIST);
            when(networkServicesMock.getVlanSupport()).thenReturn(vlanSupportMock);
            when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(server.getNovaFloatingIp()).thenCallRealMethod();
            when(server.getQuantum()).thenCallRealMethod();
            when(server.toVirtualMachine(any(JSONObject.class), anyList(), anyList(), anyList())).thenCallRealMethod();
            when(server.listVirtualMachines()).thenCallRealMethod();
            Iterable<VirtualMachine> vm = server.listVirtualMachines();
            assertNotNull("List of virtual machines cannot be null", vm);
        }
        catch( JSONException | InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }
    @Test
    public void listVirtualMachineStatusTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        Quantum quantum = mock(Quantum.class);
        JSONObject json = readJson("nova/fixtures/compute/list_servers.json");
        when(server.getMethod()).thenReturn(method);
        try {
            when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
            when(server.toStatus(any(JSONObject.class))).thenCallRealMethod();
            when(server.listVirtualMachineStatus()).thenCallRealMethod();
            Iterable<ResourceStatus> vm = server.listVirtualMachineStatus();
            assertNotNull("List of virtual machine's statuses  cannot be null", vm);
        }
        catch( JSONException | InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void pauseTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);
        VirtualMachineCapabilities capabilities = mock(VirtualMachineCapabilities.class);

        final String testVmId = "testVmId";
        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getCapabilities()).thenReturn(capabilities);
            when(capabilities.supportsPause()).thenReturn(true);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(null);
            Mockito.doCallRealMethod().when(server).pause(anyString());

            server.pause(testVmId);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).postServers(anyString(), vmIdArg.capture(), any(JSONObject.class), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testVmId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void prepareFirewallsForLaunchTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        Firewall firewall = mock(Firewall.class);
        NovaSecurityGroup securityGroups = mock(NovaSecurityGroup.class);
        FirewallSupport support = mock(FirewallSupport.class);
        final String firewallId = "testFirewallId";
        final String testProductId = "productId";
        final String testMachineImageId = "70a599e0-31e7-49b7-b260-868f441e862b";
        final String testVmName = "machineName";
        final String testVmDescription = "machineDescription";
        final VirtualMachine vm = mock(VirtualMachine.class);

        VMLaunchOptions options = VMLaunchOptions.getInstance(testProductId, testMachineImageId, testVmName, testVmDescription);
        when(server.getMethod()).thenReturn(method);
        try {
            when(support.getFirewall(firewallId)).thenReturn(firewall);
            when(firewall.getName()).thenReturn("testFirewallName");
            when(server.getNovaSecurityGroup()).thenReturn(securityGroups);
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void rebootTest () {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        final String testVmId = "testVmId";
        try {
            when(server.getMethod()).thenReturn(method);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(null);
            Mockito.doCallRealMethod().when(server).reboot(anyString());
            server.reboot(testVmId);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).postServers(anyString(), vmIdArg.capture(), any(JSONObject.class), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testVmId, vmIdArg.getValue());

        }
        catch( InternalException e ) {
            e.printStackTrace();
        }
        catch( CloudException e ) {
            e.printStackTrace();
        }
    }

    @Test
    public void resumeTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);
        VirtualMachineCapabilities capabilities = mock(VirtualMachineCapabilities.class);

        final String testVmId = "testVmId";
        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getCapabilities()).thenReturn(capabilities);
            when(capabilities.supportsResume()).thenReturn(true);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(null);
            Mockito.doCallRealMethod().when(server).resume(anyString());

            server.resume(testVmId);
            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).postServers(anyString(), vmIdArg.capture(), any(JSONObject.class), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testVmId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void startTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);
        VirtualMachineCapabilities capabilities = mock(VirtualMachineCapabilities.class);
        final String testVmId = "testVmId";
        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getCapabilities()).thenReturn(capabilities);
            when(capabilities.supportsStart()).thenReturn(true);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(null);
            Mockito.doCallRealMethod().when(server).start(anyString());

            server.start(testVmId);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).postServers(anyString(), vmIdArg.capture(), any(JSONObject.class), anyBoolean());
            assertEquals("VM Id passed to the method is not as expected", testVmId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void stopTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);
        VirtualMachineCapabilities capabilities = mock(VirtualMachineCapabilities.class);

        final String testVmId = "testVmId";

        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getCapabilities()).thenReturn(capabilities);
            when(capabilities.supportsStop()).thenReturn(true);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(null);
            Mockito.doCallRealMethod().when(server).stop(anyString(), anyBoolean());
            server.stop(testVmId, true);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).postServers(anyString(), vmIdArg.capture(), any(JSONObject.class), anyBoolean());
            assertEquals("VM Id passed to the method is not as expected", testVmId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void suspendTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);
        VirtualMachineCapabilities capabilities = mock(VirtualMachineCapabilities.class);

        final String testVmId = "testVmId";
        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getCapabilities()).thenReturn(capabilities);
            when(capabilities.supportsSuspend()).thenReturn(true);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(null);
            Mockito.doCallRealMethod().when(server).suspend(anyString());

            server.suspend(testVmId);
            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).postServers(anyString(), vmIdArg.capture(), any(JSONObject.class), anyBoolean());
            assertEquals("VM Id passed to the method is not as expected", testVmId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void unpauseTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);
        VirtualMachineCapabilities capabilities = mock(VirtualMachineCapabilities.class);

        final String testVmId = "testVmId";
        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getCapabilities()).thenReturn(capabilities);
            when(capabilities.supportsUnPause()).thenReturn(true);
            when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(null);

            Mockito.doCallRealMethod().when(server).unpause(anyString());

            server.unpause(testVmId);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).postServers(anyString(), vmIdArg.capture(), any(JSONObject.class), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testVmId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void terminateTest() {
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        Quantum quantum = mock(Quantum.class);
        VirtualMachine virtualMachine = mock(VirtualMachine.class);
        final String testPortId = "testPortId";
        List<String> ports = Arrays.asList(testPortId);
        final String testVmId = "testVmId";

        try {
            when(server.getMethod()).thenReturn(method);
            when(server.getVirtualMachine(testVmId)).thenReturn(virtualMachine);
            when(server.getQuantum()).thenReturn(quantum);
            when(quantum.listPorts(any(VirtualMachine.class))).thenReturn(ports);
            when(virtualMachine.getTag(anyString())).thenReturn(testPortId);
            Mockito.doNothing().when(method).deleteServers(anyString(), anyString());
            Mockito.doCallRealMethod().when(server).terminate(anyString(), anyString());

            server.terminate(testVmId, "Die server die");

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).deleteServers(anyString(), vmIdArg.capture());
            verify(quantum, times(1)).removePort(testPortId);
            assertEquals("VM ID passed to the method is not as expected", testVmId, vmIdArg.getValue());

        }
        catch( InternalException e ) {
            e.printStackTrace();
        }
        catch( CloudException e ) {
            e.printStackTrace();
        }
    }

    @Test
    public void toProductTest() {
        NovaServer server = mock(NovaServer.class);
        JSONObject json = readJson("nova/fixtures/compute/get_flavor.json");

        try {
            when(server.toProduct(any(JSONObject.class))).thenCallRealMethod();
            VirtualMachineProduct product = server.toProduct(json.getJSONObject("flavor"));
            assertNotNull("Returned product should not be null", product);
            assertEquals("Product id is not as expected", "1", product.getProviderProductId());
            assertEquals("Product name is not as expected", "m1.tiny", product.getName());
            assertEquals("RAM size is not as expected", 512, product.getRamSize().intValue());
            assertEquals("Root volume size is not as expected", 1, product.getRootVolumeSize().intValue());
            assertEquals("CPU count should be hardcoded to 1", 1, product.getCpuCount());
            assertEquals("Description should equal to name when it is missing", product.getName(), product.getDescription());
            assertArrayEquals("Both I32 and I64 architectures should be supported",
                    new Architecture[]{Architecture.I32, Architecture.I64}, product.getArchitectures());
        }
        catch( JSONException | InternalException | CloudException e ) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void toStatusTest() {
        NovaServer server = mock(NovaServer.class);
        JSONObject json = readJson("nova/fixtures/compute/get_server.json");

        try {
            when(server.toStatus(any(JSONObject.class))).thenCallRealMethod();
            ResourceStatus status = server.toStatus(json.getJSONObject("server"));
            assertEquals("Vm state is not as expected", VmState.RUNNING, status.getResourceStatus());
            assertEquals("Vm Id is not as expected", "7838ff1b-b71f-48b9-91e9-7c08de20b249", status.getProviderResourceId());

        }
        catch( JSONException | InternalException | CloudException e ) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void toVirtualMachineTest () {
        NovaServer server = mock(NovaServer.class);
        JSONObject json = readJson("nova/fixtures/compute/get_server.json");
        IpAddress testIp1 = new IpAddress();
        testIp1.setAddress("10.0.0.1");
        testIp1.setAddressType(AddressType.PRIVATE);
        testIp1.setIpAddressId("testId");
        testIp1.setVersion(IPVersion.IPV4);
        final List<IpAddress> ipV4 = Arrays.asList(testIp1);

        VLAN testVlan = new VLAN();
        testVlan.setName("Test VLAN");
        testVlan.setProviderVlanId("testVlanId");
        final List<VLAN> vlans = Arrays.asList(testVlan);

        try {
            when(server.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
            when(server.listFirewalls(anyString(), any(JSONObject.class))).thenReturn(Collections.EMPTY_LIST);

            when(server.toVirtualMachine(any(JSONObject.class), anyCollection(), anyCollection(), anyCollection())).thenCallRealMethod();
            VirtualMachine vm = server.toVirtualMachine(json.getJSONObject("server"), ipV4, Collections.EMPTY_LIST, vlans);
            assertNotNull("Returned vm should not be null", vm);
            assertEquals("Architecture is not as expected", Architecture.I64, vm.getArchitecture());
            assertEquals("Affinity Group Id is not as expected", null, vm.getAffinityGroupId());
            assertEquals("Current State is not as expected", VmState.RUNNING, vm.getCurrentState());
            assertArrayEquals("Labels are not as expected", new String[0], vm.getLabels());
            assertEquals("Description is not as expected", "new-server-test", vm.getDescription());
            assertEquals("Name is not as expected", "new-server-test", vm.getName());
            assertEquals("Platform is not as expected", null, vm.getPlatform());
            assertEquals("Product Id is not as expected", "1", vm.getProductId());
            assertEquals("Provider Machine Image Id is not as expected", "70a599e0-31e7-49b7-b260-868f441e862b", vm.getProviderMachineImageId());
            assertEquals("Provider Owner Id is not as expected", null, vm.getProviderOwnerId());
            assertEquals("Provider Ramdisk Image Id is not as expected", null, vm.getProviderRamdiskImageId());
            assertEquals("Provider Region Id is not as expected", null, vm.getProviderRegionId());
            assertEquals("Provider Subnet Id is not as expected", null, vm.getProviderSubnetId());
            assertEquals("Provider Virtual Machine Id is not as expected", "7838ff1b-b71f-48b9-91e9-7c08de20b249", vm.getProviderVirtualMachineId());
            assertEquals("Provider Vlan Id is not as expected", null, vm.getProviderVlanId());
            assertEquals("Provider Keypair Id is not as expected", null, vm.getProviderKeypairId());
            assertEquals("VirtualMachineGroup is not as expected", null, vm.getVirtualMachineGroup());
            assertEquals("Visible Scope is not as expected", null, vm.getVisibleScope());
            assertEquals("Resource Pool Id is not as expected", null, vm.getResourcePoolId());
            assertEquals("Provider VM Status is not as expected", null, vm.getProviderVmStatus());
            assertEquals("Provider Host Status is not as expected", null, vm.getProviderHostStatus());
            assertArrayEquals("Volumes is not as expected", null, vm.getVolumes());
        }
        catch( JSONException e ) {
            e.printStackTrace();
        }
        catch( InternalException e ) {
            e.printStackTrace();
        }
        catch( CloudException e ) {
            e.printStackTrace();
        }

    }
}
