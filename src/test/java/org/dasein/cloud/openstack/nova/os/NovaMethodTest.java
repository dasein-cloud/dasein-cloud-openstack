package org.dasein.cloud.openstack.nova.os;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.openstack.nova.os.compute.NovaServer;
import org.dasein.cloud.openstack.nova.os.ext.hp.cdn.HPCDN;
import org.dasein.cloud.openstack.nova.os.network.Quantum;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by mariapavlova on 20/11/2015.
 */
public class NovaMethodTest {

    @Test
    public void deleteServersTest (){
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testComputeUrl = "testComputeUrl";
        final String testAuthToken = "testAuthToken";
        final String testResource = "testResource";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getComputeUrl()).thenReturn(testComputeUrl);

            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doNothing().when(method).delete(anyString(),anyString(),anyString());
            Mockito.doCallRealMethod().when(method).deleteServers(anyString(), anyString());
            method.deleteServers(testResource, testResourceId);

            ArgumentCaptor<String> testToken = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testEndpoint = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testResourceCapt = ArgumentCaptor.forClass(String.class);
            verify(method).delete(testToken.capture(), testEndpoint.capture(), testResourceCapt.capture());
            assertEquals("Token passed to the method is not as expected", testAuthToken, testToken.getValue());
            assertEquals("Endpoint passed to the method is not as expected", testComputeUrl, testEndpoint.getValue());
            assertEquals("Resource passed to the method is not as expected", testResource + "/" + testResourceId, testResourceCapt.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void deleteNetworks (){
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testNetworkUrl = "testNetworkUrl";
        final String testAuthToken = "testAuthToken";
        final String testResource = "testResource";

        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getNetworkUrl()).thenReturn(testNetworkUrl);
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doNothing().when(method).delete(anyString(),anyString(),anyString());
            Mockito.doCallRealMethod().when(method).deleteNetworks(anyString(), anyString());
            method.deleteNetworks("testResource", testResourceId);

            ArgumentCaptor<String> testToken = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testEndpoint = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testResourceCapt = ArgumentCaptor.forClass(String.class);
            verify(method).delete(testToken.capture(), testEndpoint.capture(), testResourceCapt.capture());
            assertEquals("Token passed to the method is not as expected", testAuthToken, testToken.getValue());
            assertEquals("Endpoint passed to the method is not as expected", testNetworkUrl + "/", testEndpoint.getValue());
            assertEquals("Resource passed to the method is not as expected", testResource + "/" + testResourceId, testResourceCapt.getValue());


        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getPortsTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testComputeUrl = "testComputeUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getComputeUrl()).thenReturn(testComputeUrl);
            when(context.getMyRegion()).thenReturn("testRegionId");
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).getPorts(anyString(), anyString());
            method.getPorts("testResource", testResourceId);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).getPorts(anyString(), vmIdArg.capture());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getServersTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testComputeUrl = "testComputeUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getComputeUrl()).thenReturn(testComputeUrl);
            when(context.getMyRegion()).thenReturn("testRegionId");
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).getServers(anyString(), anyString(), anyBoolean());
            method.getServers("testResource", testResourceId, true);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).getServers(anyString(), vmIdArg.capture(), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getNetworksTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testNetworkUrl = "testNetworkUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getNetworkUrl()).thenReturn(testNetworkUrl);
            when(context.getMyRegion()).thenReturn("testRegionId");
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).getNetworks(anyString(), anyString(), anyBoolean(), anyString());
            method.getNetworks("testResource", testResourceId, true, "testQuery");

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).getNetworks(anyString(), vmIdArg.capture(), anyBoolean(), anyString());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());

        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void postServersForStringTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);
        JSONObject serverObj = new JSONObject();

        final String testResourceId = "testResourceId";
        final String testComputeUrl = "testComputeUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getComputeUrl()).thenReturn(testComputeUrl);
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).postServersForString(anyString(), anyString(), any(JSONObject.class), anyBoolean());
            method.postServersForString("testResource", testResourceId, serverObj, true);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<JSONObject> jsonArg = ArgumentCaptor.forClass(JSONObject.class);

            verify(method).postServersForString(anyString(), vmIdArg.capture(), jsonArg.capture(), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void postServersTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);
        JSONObject serverObj = new JSONObject();

        final String testResourceId = "testResourceId";
        final String testComputeUrl = "testComputeUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getComputeUrl()).thenReturn(testComputeUrl);
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean());
            method.postServers("testResource", testResourceId, serverObj, true);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<JSONObject> jsonArg = ArgumentCaptor.forClass(JSONObject.class);

            verify(method).postServers(anyString(), vmIdArg.capture(), jsonArg.capture(), anyBoolean());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void postNetworksTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);
        JSONObject serverObj = new JSONObject();

        final String testResourceId = "testResourceId";
        final String testNetworkUrl = "testNetworkUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getNetworkUrl()).thenReturn(testNetworkUrl);
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).postNetworks(anyString(), anyString(), any(JSONObject.class), anyString());
            method.postNetworks("testResource", testResourceId, serverObj, "testAction");

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<JSONObject> jsonArg = ArgumentCaptor.forClass(JSONObject.class);

            verify(method).postNetworks(anyString(), vmIdArg.capture(), jsonArg.capture(), anyString());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void putNetworksTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);
        JSONObject serverObj = new JSONObject();

        final String testResourceId = "testResourceId";
        final String testNetworkUrl = "testNetworkUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getNetworkUrl()).thenReturn(testNetworkUrl);
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).putNetworks(anyString(), anyString(), any(JSONObject.class), anyString());
            method.putNetworks("testResource", testResourceId, serverObj, "testAction");

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<JSONObject> jsonArg = ArgumentCaptor.forClass(JSONObject.class);

            verify(method).putNetworks(anyString(), vmIdArg.capture(), jsonArg.capture(), anyString());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void getHPCDNTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testServiceUrl = "testServiceUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getServiceUrl(anyString())).thenReturn(testServiceUrl);
            when(context.getMyRegion()).thenReturn("testRegionId");
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).getHPCDN(anyString());
            method.getHPCDN(testResourceId);

            ArgumentCaptor<String> vmIdArg = ArgumentCaptor.forClass(String.class);
            verify(method).getHPCDN(vmIdArg.capture());
            assertEquals("VM ID passed to the method is not as expected", testResourceId, vmIdArg.getValue());

        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void putHPCDNTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testServiceUrl = "testServiceUrl";
        final String testAuthToken = "testAuthToken";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getServiceUrl(anyString())).thenReturn(testServiceUrl);
            when(context.getMyRegion()).thenReturn("testRegionId");
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).putHPCDN(anyString());
            when(method.headResource(anyString(), anyString(), anyString())).thenReturn(Collections.EMPTY_MAP);

            method.putHPCDN(testResourceId);

            ArgumentCaptor<Map> headers = ArgumentCaptor.forClass(Map.class);
            verify(method).putHeaders(anyString(), anyString(), anyString(), headers.capture());

            Map<String, String> verifyMap = headers.getValue();
            assertEquals("X-TTL param is not present", true, verifyMap.containsKey("X-TTL"));
            assertEquals("X-TTL param value is not as expected", "86400", verifyMap.get("X-TTL"));

            ArgumentCaptor<String> service = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resource = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> resourceId = ArgumentCaptor.forClass(String.class);
            verify(method).headResource(service.capture(), resource.capture(), resourceId.capture());
            assertEquals("Service is not as expected", HPCDN.SERVICE, service.getValue());
            assertEquals("Resource is not as expected", HPCDN.RESOURCE, resource.getValue());
            assertEquals("ResourceId is not as expected", testResourceId, resourceId.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

    @Test
    public void postHPCDNTest () {
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testResourceId = "testResourceId";
        final String testRegionId = "testRegionId";
        final String testAuthToken = "testAuthToken";
        final String testComputeUrl = "testComputeUrl";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getServiceUrl(anyString())).thenReturn(testComputeUrl);
            when(context.getMyRegion()).thenReturn(testRegionId);
            when(context.getAuthToken()).thenReturn(testAuthToken);
            Mockito.doCallRealMethod().when(method).postHPCDN(anyString(), anyMap());
            method.postHPCDN(testResourceId, Collections.EMPTY_MAP);

            ArgumentCaptor<String> testToken = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testEndpoint = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testResource = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Map> testMap = ArgumentCaptor.forClass(Map.class);
            verify(method).postHeaders(testToken.capture(), testEndpoint.capture(), testResource.capture(), testMap.capture());
            assertEquals("Token passed to the method is not as expected", testAuthToken, testToken.getValue());
            assertEquals("Endpoint passed to the method is not as expected", testComputeUrl, testEndpoint.getValue());
            assertEquals("Resource passed to the method is not as expected", "/" + testResourceId, testResource.getValue());
            assertEquals("Custom headers passed to the method are not as expected", Collections.EMPTY_MAP, testMap.getValue());
        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }
    @Test
    public void deleteHPCDNTest (){
        AuthenticationContext context = mock(AuthenticationContext.class);
        NovaMethod method = mock(NovaMethod.class);

        final String testContainerId = "testContainerId";
        final String testServiceUrl = "testServiceUrl";
        final String testAuthToken = "testAuthToken";
        final String testRegionId = "testRegionId";
        try {
            when(method.getAuthenticationContext()).thenReturn(context);
            when(context.getServiceUrl(anyString())).thenReturn(testServiceUrl);
            when(context.getAuthToken()).thenReturn(testAuthToken);
            when(context.getMyRegion()).thenReturn(testRegionId);
            Mockito.doNothing().when(method).delete(anyString(),anyString(),anyString());
            Mockito.doCallRealMethod().when(method).deleteHPCDN(anyString());
            method.deleteHPCDN("testContainerId");

            ArgumentCaptor<String> testToken = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testEndpoint = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> testContainer = ArgumentCaptor.forClass(String.class);
            verify(method).delete(testToken.capture(), testEndpoint.capture(), testContainer.capture());
            assertEquals("Token passed to the method is not as expected", testAuthToken, testToken.getValue());
            assertEquals("Endpoint passed to the method is not as expected", testServiceUrl, testEndpoint.getValue());
            assertEquals("Resource passed to the method is not as expected", "/" + testContainerId, testContainer.getValue());

        }
        catch( InternalException | CloudException e) {
            e.printStackTrace();
            fail("Test failed " + e.getMessage());
        }
    }

}
