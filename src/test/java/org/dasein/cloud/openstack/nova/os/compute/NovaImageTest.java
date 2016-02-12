package org.dasein.cloud.openstack.nova.os.compute;

import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.OpenStackProvider;
import org.dasein.cloud.openstack.nova.os.OpenStackTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by mariapavlova on 12/02/2016.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(NovaImage.class)
public class NovaImageTest extends OpenStackTest {
    private NovaMethod          method;
    private NovaImage           imageSupport;
    private NovaComputeServices computeServices;
    private NovaServer          vmSupport;

    private String testImageId = "testImageId";
    private String testImageName = "testImageName";
    private String testImageDescription = "testImageDescription";
    private String testVmId = "testVmId";
    private String testRegionId = "testRegionId";

    @Before
    public void setup() {
        NovaOpenStack provider = mock(NovaOpenStack.class);
        try {
            when(provider.isPostCactus()).thenReturn(true);
            when(provider.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
        }
        catch( CloudException | InternalException e ) {
            e.printStackTrace();
            fail();
        }
        ProviderContext context = mock(ProviderContext.class);
        when(context.getRegionId()).thenReturn(testRegionId);
        try {
            PowerMockito.doReturn(provider).when(imageSupport, "getProvider");
            PowerMockito.doReturn(context).when(imageSupport, "getContext");
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Exception occurred " + e.getMessage());
        }

        imageSupport = mock(NovaImage.class);
        computeServices = mock(NovaComputeServices.class);
        vmSupport = mock(NovaServer.class);
        method = mock(NovaMethod.class);
        when(imageSupport.getMethod()).thenReturn(method);
        when(imageSupport.getComputeServices()).thenReturn(computeServices);
        when(computeServices.getVirtualMachineSupport()).thenReturn(vmSupport);
    }

    @Test
    public void testGetImageRef() throws Exception {
        // prepare mocks and data
        JSONObject json = readJson("nova/fixtures/images/get_image.json");
        when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(imageSupport.getImageRef(anyString())).thenCallRealMethod();

        // test invocation
        String ref = imageSupport.getImageRef(testImageId);

        // verify
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        verify(method).getServers(anyString(), imageIdCapt.capture(), anyBoolean());
        assertEquals("Image Id parameter is not as expected", testImageId, imageIdCapt.getValue());
        assertEquals("Returned value is not as expected",
                "http://nova-osp7/v2/94b7d98054db4d4eb5734e4a370805c7/images/11cdc38b-7d87-4ab9-86bd-19cf33056b81",
                ref);
    }

    @Test
    public void testCapture() throws Exception {
        // prepare mocks and data
        JSONObject json = readJson("nova/fixtures/images/get_image.json");
        VirtualMachine vm = mock(VirtualMachine.class);
        when(vm.getProviderVirtualMachineId()).thenReturn(testVmId);
        when(vm.getPlatform()).thenReturn(Platform.CENT_OS);
        when(vmSupport.getVirtualMachine(anyString())).thenReturn(vm);
        when(method.postServers(anyString(), anyString(), any(JSONObject.class), anyBoolean())).thenReturn(json);
        when(imageSupport.capture(any(ImageCreateOptions.class), any(AsynchronousTask.class))).thenCallRealMethod();
        when(imageSupport.toImage(any(JSONObject.class))).thenCallRealMethod();

        // test invocation
        MachineImage machineImage = imageSupport.capture(ImageCreateOptions.getInstance(vm, testImageName, testImageDescription), null);

        // verify
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<JSONObject> jsonCapt = ArgumentCaptor.forClass(JSONObject.class);
        ArgumentCaptor<Boolean> suffixCapt = ArgumentCaptor.forClass(Boolean.class);

        verify(method).postServers(resourceCapt.capture(), resourceIdCapt.capture(), jsonCapt.capture(), suffixCapt.capture());
        assertEquals("Requested resource is invalid", "/servers", resourceCapt.getValue());
        assertEquals("Requested resourceId is invalid", testVmId, resourceIdCapt.getValue());
        assertTrue("Requested JSON is invalid", jsonCapt.getValue().has("createImage"));
        assertTrue("Requested suffix flag is invalid", suffixCapt.getValue());
        assertNotNull("Returned machine image is invalid", machineImage);
    }

    @Test
    public void testGetImage() throws Exception {
//        fail("Test is not ready");
    }

    @Test
    public void testIsImageSharedWithPublic() throws Exception {
//        fail("Test is not ready");
    }

    @Test public void testIsSubscribed() throws Exception {

    }

    @Test public void testListImageStatus() throws Exception {

    }

    @Test public void testListImages() throws Exception {

    }

    @Test public void testRemove() throws Exception {

    }

    @Test public void testSearchPublicImages() throws Exception {

    }

    @Test public void testToImage() throws Exception {

    }

    @Test public void testToStatus() throws Exception {

    }

    @Test public void testSetTags() throws Exception {

    }

    @Test public void testSetTags1() throws Exception {

    }

    @Test public void testUpdateTags() throws Exception {

    }

    @Test public void testUpdateTags1() throws Exception {

    }

    @Test public void testRemoveTags() throws Exception {

    }

    @Test public void testRemoveTags1() throws Exception {

    }
}