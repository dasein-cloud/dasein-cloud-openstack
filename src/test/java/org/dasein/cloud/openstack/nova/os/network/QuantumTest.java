package org.dasein.cloud.openstack.nova.os.network;

import org.apache.commons.io.IOUtils;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.VisibleScope;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by mariapavlova on 15/09/2015.
 */
public class QuantumTest {
    @Test
    public void listVlansTest() {
        NovaMethod method = mock(NovaMethod.class);
        Quantum quantum = mock(Quantum.class);
        InputStream is = getClass().getClassLoader().getResourceAsStream("nova/fixtures/list_vlans.json");

        JSONObject json = null;
        try {
            String jsonText = IOUtils.toString(is);
            json = new JSONObject(jsonText);
        }
        catch( IOException e ) {
            throw new RuntimeException(e);
        }
        catch( JSONException e ) {
            throw new RuntimeException(e);
        }
        when(quantum.getMethod()).thenReturn(method);
        try {
            when(quantum.getNetworkType()).thenReturn(Quantum.QuantumType.QUANTUM);
            when(quantum.getTenantId()).thenReturn("628b7b037c8a43ef8868327c0accda40");
            when(quantum.getCurrentRegionId()).thenReturn("RegionOne");
            when(method.getNetworks(anyString(), anyString(), anyBoolean())).thenReturn(json);
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
        JSONObject json = null;
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream("nova/fixtures/list_ports.json");
            String jsonText = IOUtils.toString(is);
            json = new JSONObject(jsonText);
        }
        catch( IOException e ) {
            throw new RuntimeException(e);
        }
        catch( JSONException e ) {
            throw new RuntimeException(e);
        }
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
}
