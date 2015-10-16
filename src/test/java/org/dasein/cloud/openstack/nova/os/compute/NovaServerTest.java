package org.dasein.cloud.openstack.nova.os.compute;

import org.apache.commons.io.IOUtils;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.VLANSupport;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.OpenStackProvider;
import org.dasein.cloud.openstack.nova.os.network.Quantum;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

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
        catch( JSONException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( InternalException e ) {
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
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( CloudException e ) {
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
    public void getVirtualMachineTest(){
        NovaMethod method = mock(NovaMethod.class);
        NovaServer server = mock(NovaServer.class);
        NetworkServices networkServicesMock = mock(NetworkServices.class);
        IpAddressSupport ipAddressSupportMock = mock(IpAddressSupport.class);
        VLANSupport vlanSupportMock = mock(VLANSupport.class);

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
        catch( InternalException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( CloudException e ) {
            fail("Test failed " + e.getMessage());
        }
        catch( JSONException e ) {
            fail("Test failed " + e.getMessage());
        }
    }


    }
