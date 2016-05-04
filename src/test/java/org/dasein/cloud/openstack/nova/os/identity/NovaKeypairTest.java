package org.dasein.cloud.openstack.nova.os.identity;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.identity.SSHKeypair;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.OpenStackProvider;
import org.dasein.cloud.openstack.nova.os.OpenStackTest;
import org.dasein.cloud.openstack.nova.os.compute.NovaComputeServices;
import org.dasein.cloud.openstack.nova.os.compute.NovaServer;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by mariapavlova on 02/03/2016.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(NovaKeypair.class)
public class NovaKeypairTest extends OpenStackTest {
    private NovaMethod  method;
    private NovaKeypair keypairSupport;

    private     NovaOpenStack       provider;

    private String testKeypairId = "keypair-50ca852e-273f-4cdc-8949-45feba200837";
    private String testRegionId = "testRegionId";
    private String testOwnerId = "5ef70662f8b34079a6eddb8da9d75fe8";
    private String testKeypairName = "testKeypairName";
    private String testPublicKey = "testPublicKey";

    @Before
    public void setup() {
        provider = PowerMockito.mock(NovaOpenStack.class);
        try {
            PowerMockito.when(provider.isPostCactus()).thenReturn(true);
            PowerMockito.when(provider.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
        }
        catch( CloudException | InternalException e ) {
            e.printStackTrace();
            fail();
        }
        ProviderContext context = PowerMockito.mock(ProviderContext.class);
        PowerMockito.when(context.getRegionId()).thenReturn(testRegionId);
        PowerMockito.when(context.getAccountNumber()).thenReturn(testOwnerId);

        keypairSupport = PowerMockito.mock(NovaKeypair.class);

        try {
            PowerMockito.doReturn(provider).when(keypairSupport, "getProvider");
            PowerMockito.doReturn(context).when(keypairSupport, "getContext");
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Exception occurred " + e.getMessage());
        }

        method = PowerMockito.mock(NovaMethod.class);
        try {
            whenNew(NovaMethod.class).withAnyArguments().thenReturn(method);
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Couldn't create a mock for NovaMethod construction");
        }
    }

    @Test
    public void testCreateKeypair() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/create_keypair.json");
        SSHKeypair keypair = new SSHKeypair();
        keypair.setProviderKeypairId(testKeypairId);

        when(keypairSupport.toKeypair(any(JSONObject.class))).thenReturn(keypair);
        when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(json);
        when(keypairSupport.createKeypair(anyString())).thenCallRealMethod();

        SSHKeypair res = keypairSupport.createKeypair("testKeypairName");

        assertNotNull("Returned keipair name cannot be null", res);
        assertEquals("Returned keipair name is not as expected", keypair, res);

    }

    @Test
    public void testDeleteKeypair() throws Exception {
        Mockito.doNothing().when(method).deleteServers(anyString(), anyString());
        Mockito.doCallRealMethod().when(keypairSupport).deleteKeypair(anyString());

        keypairSupport.deleteKeypair(testKeypairId);

        ArgumentCaptor<String> keypairIdArg = ArgumentCaptor.forClass(String.class);
        verify(method).deleteServers(anyString(), keypairIdArg.capture());
        assertEquals("Keypair ID passed to the method is not as expected", testKeypairId, keypairIdArg.getValue());

    }

    @Test
    public void testGetFingerprint() throws Exception {
        SSHKeypair keypair = new SSHKeypair();
        keypair.setFingerprint("fingerprint");

        when(keypairSupport.getKeypair(anyString())).thenReturn(keypair);
        when(keypairSupport.getFingerprint(anyString())).thenCallRealMethod();

        String res = keypairSupport.getFingerprint(testKeypairId);

        assertNotNull("Returned fingerprint id cannot be null", res);
        assertEquals("Fingerprint is not as expected", "fingerprint", res);
    }

    @Test
    public void testGetKeypair() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/list_keypairs.json");
        when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(keypairSupport.toKeypair(any(JSONObject.class))).thenCallRealMethod();
        Mockito.doCallRealMethod().when(keypairSupport).getKeypair(anyString());

        SSHKeypair res = keypairSupport.getKeypair(testKeypairId);

        assertNotNull("Returned keypair id cannot be null", res);
        assertEquals("Returned keipair fingerprint is not as expected",
                "7e:eb:ab:24:ba:d1:e1:88:ae:9a:fb:66:53:df:d3:bd",
                res.getFingerprint());

    }

    @Test
    public void testGetCapabilities() throws Exception {

    }

    @Test
    public void testImportKeypair() throws Exception {
        SSHKeypair keypair = new SSHKeypair();

        JSONObject json = readJson("nova/fixtures/compute/create_keypair.json");
        when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(json);

        when(keypairSupport.toKeypair(any(JSONObject.class))).thenCallRealMethod();
        when(keypairSupport.importKeypair(anyString(), anyString())).thenCallRealMethod();

        SSHKeypair res = keypairSupport.importKeypair(testKeypairName, testPublicKey);
        assertNotNull("Returned keypair id cannot be null", res);

        ArgumentCaptor<JSONObject> jsonArg = ArgumentCaptor.forClass(JSONObject.class);

        verify(method).postServers(anyString(), anyString(), jsonArg.capture(), anyBoolean());
        Map<String, Object> keypairParam = ( Map<String, Object> ) jsonArg.getValue().get("keypair");
        assertNotNull("Argument is incorrect", keypairParam);
        assertEquals("Argument value 'name' is incorrect", testKeypairName, keypairParam.get("name"));
        assertEquals("Argument value 'public_key' is incorrect", testPublicKey, keypairParam.get("public_key"));
    }

    @Test
    public void testIsSubscribed() throws Exception {

    }

    @Test
    public void testList() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/list_keypairs.json");
        when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(keypairSupport.toKeypair(any(JSONObject.class))).thenCallRealMethod();
        when(keypairSupport.list()).thenCallRealMethod();

        Collection<SSHKeypair> res = keypairSupport.list();
        int count = 0;
        for( SSHKeypair key : res ) {
            count++;
        }
        assertNotNull("Returned list cannot be null", res);
        assertEquals("Returned number of keypairs is not as expected", 1, count);
    }

}