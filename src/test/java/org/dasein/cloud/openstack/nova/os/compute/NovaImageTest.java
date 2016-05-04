package org.dasein.cloud.openstack.nova.os.compute;

import org.dasein.cloud.*;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.OpenStackProvider;
import org.dasein.cloud.openstack.nova.os.OpenStackTest;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

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
    private NovaOpenStack       provider;

    private String testImageId = "testImageId";
    private String testImageName = "testImageName";
    private String testImageDescription = "testImageDescription";
    private String testVmId = "testVmId";
    private String testRegionId = "testRegionId";
    private String testOwnerId = "5ef70662f8b34079a6eddb8da9d75fe8";

    @Before
    public void setup() {
        provider = mock(NovaOpenStack.class);
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
        when(context.getAccountNumber()).thenReturn(testOwnerId);

        imageSupport = mock(NovaImage.class);

        try {
            PowerMockito.doReturn(provider).when(imageSupport, "getProvider");
            PowerMockito.doReturn(context).when(imageSupport, "getContext");
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Exception occurred " + e.getMessage());
        }

        computeServices = mock(NovaComputeServices.class);
        vmSupport = mock(NovaServer.class);
        method = mock(NovaMethod.class);
        try {
            whenNew(NovaMethod.class).withAnyArguments().thenReturn(method);
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Couldn't create a mock for NovaMethod construction");
        }
        when(imageSupport.getMethod()).thenReturn(method);
        when(imageSupport.getComputeServices()).thenReturn(computeServices);
        try {
            when(imageSupport.getTenantId()).thenReturn(testOwnerId);
        }
        catch( CloudException | InternalException e ) {
            e.printStackTrace();
            fail("Some kind of a problem while getting a tenant id");
        }
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
        // prepare mocks and data
        JSONObject json = readJson("nova/fixtures/images/get_image.json");
        MachineImage machineImage = mock(MachineImage.class);

        when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(imageSupport.toImage(any(JSONObject.class))).thenReturn(machineImage);
        when(imageSupport.getImage(anyString())).thenCallRealMethod();

        // test invocation
        MachineImage machineImage1 = imageSupport.getImage(testImageId);

        // verify
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        verify(method).getServers(anyString(), imageIdCapt.capture(), anyBoolean());
        assertEquals("Image Id parameter is not as expected", testImageId, imageIdCapt.getValue());
        ArgumentCaptor<JSONObject> jsonObjectArgumentCaptor = ArgumentCaptor.forClass(JSONObject.class);
        verify(imageSupport).toImage(jsonObjectArgumentCaptor.capture());
        assertEquals("Json parameter not as expected", json.getJSONObject("image"), jsonObjectArgumentCaptor.getValue());
        assertEquals("Returned value is not as expected",
                machineImage, machineImage1);
    }

    @Test
    public void testIsImageSharedWithPublic() throws Exception {
        MachineImage machineImage = MachineImage.getInstance(testOwnerId, testRegionId, testImageId,
                ImageClass.MACHINE, MachineImageState.ACTIVE, "testName", "testDescription",
                Architecture.I64, Platform.CENT_OS);
        when(imageSupport.getImage(anyString())).thenReturn(machineImage);
        when(imageSupport.isImageSharedWithPublic(anyString())).thenCallRealMethod();

        // test invocation
        boolean test = imageSupport.isImageSharedWithPublic(testImageId);
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        verify(imageSupport).getImage(imageIdCapt.capture());
        assertEquals("getImage parameter is not as expected", testImageId, imageIdCapt.getValue());
        assertFalse("Result value is incorrect", test);
    }

    @Test
    public void testIsSubscribed() throws Exception {
        when(provider.testContext()).thenReturn(testOwnerId);
        when(imageSupport.isSubscribed()).thenCallRealMethod();
        boolean test = imageSupport.isSubscribed();
        assertTrue("Return value is incorrect", test);
    }

    @Test
    public void testListImageStatus() throws Exception {
        JSONObject json = readJson("nova/fixtures/images/get_images.json");
        when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
        ResourceStatus dummy = new ResourceStatus("dummy", MachineImageState.ACTIVE);
        when(imageSupport.toStatus(any(JSONObject.class))).thenReturn(dummy);
        when(imageSupport.listImageStatus(any(ImageClass.class))).thenCallRealMethod();

        Iterator<ResourceStatus> resourceStatuses = imageSupport.listImageStatus(ImageClass.MACHINE).iterator();
        verify(imageSupport, times(11)).toStatus(any(JSONObject.class));
        int count = 0;
        while( resourceStatuses.hasNext() ) {
            count++;
            resourceStatuses.next();
        }
        assertEquals("The number of returned objects is incorrect", 11, count);
    }

    @Test
    public void testListImages() throws Exception {
        JSONObject json = readJson("nova/fixtures/images/get_images.json");
        when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
        MachineImage dummy = MachineImage.getInstance(testOwnerId, testRegionId, "dummy", ImageClass.MACHINE, MachineImageState.ACTIVE, "dummy", "dummy", Architecture.I64, Platform.CENT_OS);
        when(imageSupport.toImage(any(JSONObject.class))).thenReturn(dummy);
        when(imageSupport.listImages(any(ImageFilterOptions.class))).thenCallRealMethod();

        // test invocation
        Iterator<MachineImage> images = imageSupport.listImages(ImageFilterOptions.getInstance()).iterator();
        verify(imageSupport, times(11)).toImage(any(JSONObject.class));
        int count = 0;
        while( images.hasNext() ) {
            count++;
            images.next();
        }
        assertEquals("The number of returned objects is incorrect", 11, count);
    }

    @Test
    public void testRemove() throws Exception {
        Mockito.doCallRealMethod().when(imageSupport).remove(anyString(), anyBoolean());
        imageSupport.remove(testImageId, true);

        ArgumentCaptor<String> machineImageIdArg = ArgumentCaptor.forClass(String.class);
        verify(method).deleteServers(anyString(), machineImageIdArg.capture());
        assertEquals("Machine Image ID passed to the method is not as expected", testImageId, machineImageIdArg.getValue());
    }

    @Test
    public void testSearchPublicImages() throws Exception {
        JSONObject json = readJson("nova/fixtures/images/get_images.json");
        when(method.getServers(anyString(), anyString(), anyBoolean())).thenReturn(json);
        MachineImage privateImage = MachineImage.getInstance(testOwnerId, testRegionId, "dummy", ImageClass.MACHINE, MachineImageState.ACTIVE, "dummy", "dummy", Architecture.I64, Platform.CENT_OS);
        MachineImage publicImage = MachineImage.getInstance("-public-", testRegionId, "dummy", ImageClass.MACHINE, MachineImageState.ACTIVE, "dummy", "dummy", Architecture.I64, Platform.CENT_OS);
        // return private image once, then public
        when(imageSupport.toImage(any(JSONObject.class))).thenReturn(privateImage).thenReturn(publicImage);
        when(imageSupport.searchPublicImages(any(ImageFilterOptions.class))).thenCallRealMethod();

        // test invocation
        Iterator<MachineImage> images = imageSupport.searchPublicImages(ImageFilterOptions.getInstance()).iterator();
        verify(imageSupport, times(11)).toImage(any(JSONObject.class));
        int count = 0;
        while( images.hasNext() ) {
            count++;
            images.next();
        }
        assertEquals("The number of returned objects is incorrect", 10, count);
    }

    @Test
    public void testToImage() throws Exception {
        JSONObject json = readJson("nova/fixtures/images/get_image.json");
        when(imageSupport.toImage(any(JSONObject.class))).thenCallRealMethod();
        MachineImage image = imageSupport.toImage(json.getJSONObject("image"));
        assertNotNull("Returned value is incorrect", image);
        assertEquals("Minimum disk size is incorrect", 20, image.getMinimumDiskSizeGb());
        assertEquals("Architecture is incorrect", Architecture.I64, image.getArchitecture());
        assertEquals("Creation time is incorrect", 1454915463000L, image.getCreationTimestamp());
        assertEquals("Current state is incorrect", MachineImageState.ACTIVE, image.getCurrentState());
        assertEquals("Description is incorrect", "kstestubu1", image.getDescription());
        assertEquals("Image class is incorrect", ImageClass.MACHINE, image.getImageClass());
        assertEquals("Name is incorrect", "kstestubu1", image.getName());
        assertEquals("Platform is incorrect", Platform.UBUNTU, image.getPlatform());
        assertEquals("Machine image id is incorrect", "11cdc38b-7d87-4ab9-86bd-19cf33056b81", image.getProviderMachineImageId());
        assertEquals("Region id is incorrect", testRegionId, image.getProviderRegionId());
        assertEquals("Software is incorrect", "", image.getSoftware());
        assertEquals("Image type is incorrect", MachineImageType.VOLUME, image.getType());
        assertEquals("Number of tags is incorrect", 10, image.getTags().size());
        assertEquals("base_image_ref tag is incorrect", "be75c519-a938-41e1-88bb-2af6d9e9c962", image.getTag("base_image_ref"));
        assertEquals("user_id tag is incorrect", "e3ea2e865bcd43f59b97dfc76acd0e81", image.getTag("user_id"));
        assertEquals("org.dasein.description tag is incorrect", "kstestubu1", image.getTag("org.dasein.description"));
        assertEquals("owner_id tag is incorrect", "94b7d98054db4d4eb5734e4a370805c7", image.getTag("owner_id"));
        assertEquals("org.dasein.platform tag is incorrect", "UBUNTU", image.getTag("org.dasein.platform"));
        assertEquals("image_location tag is incorrect", "snapshot", image.getTag("image_location"));
        assertEquals("image_state tag is incorrect", "available", image.getTag("image_state"));
        assertEquals("network_allocated tag is incorrect", "True", image.getTag("network_allocated"));
        assertEquals("instance_uuid tag is incorrect", "405e9abf-82aa-46d2-8c94-6f14d78d0a26", image.getTag("instance_uuid"));
        assertEquals("image_type tag is incorrect", "snapshot", image.getTag("image_type"));
        assertFalse("Image should not be marked as shared with public", image.isPublic());
    }

    @Test
    public void testToStatus() throws Exception {
        JSONObject json = readJson("nova/fixtures/images/get_image.json");
        when(imageSupport.toStatus(any(JSONObject.class))).thenCallRealMethod();

        // test invocation
        ResourceStatus test = imageSupport.toStatus(json.getJSONObject("image"));

        assertNotNull("Return value is incorrect", test);
        assertEquals("Resource id is incorrect", "11cdc38b-7d87-4ab9-86bd-19cf33056b81", test.getProviderResourceId());
        assertEquals("Resource status is incorrect", MachineImageState.ACTIVE, test.getResourceStatus());
    }

    @Test
    public void testSetTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(imageSupport).setTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        imageSupport.setTags(testImageId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).createTags(serviceIdCapt.capture(), resourceCapt.capture(), imageIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", NovaImage.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/images", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testImageId, imageIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );
    }

    @Test
    public void testSetTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(imageSupport).setTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherImageId = "anotherTestId";

        // run
        imageSupport.setTags(new String[] {testImageId, anotherImageId}, tag);

        // verify
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(imageSupport, times(2)).setTags(imageIdCapt.capture(), tag1Capt.capture());
        List<String> imageIdValues = imageIdCapt.getAllValues();
        assertArrayEquals("Image ids were not correct", new String[]{testImageId, anotherImageId},
                imageIdValues.toArray(new String[imageIdValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }

    @Test
    public void testUpdateTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(imageSupport).updateTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        imageSupport.updateTags(testImageId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).updateTags(serviceIdCapt.capture(), resourceCapt.capture(), imageIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", NovaImage.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/images", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testImageId, imageIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );
    }

    @Test public void testUpdateTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(imageSupport).updateTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherImageId = "anotherTestId";

        // run
        imageSupport.updateTags(new String[] {testImageId, anotherImageId}, tag);

        // verify
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(imageSupport, times(2)).updateTags(imageIdCapt.capture(), tag1Capt.capture());
        List<String> imageIdValues = imageIdCapt.getAllValues();
        assertArrayEquals("Image ids were not correct", new String[]{testImageId, anotherImageId},
                imageIdValues.toArray(new String[imageIdValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }

    @Test
    public void testRemoveTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(imageSupport).removeTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        imageSupport.removeTags(testImageId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).removeTags(serviceIdCapt.capture(), resourceCapt.capture(), imageIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", NovaImage.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/images", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testImageId, imageIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );

    }

    @Test
    public void testRemoveTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(imageSupport).removeTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherImageId = "anotherTestId";

        // run
        imageSupport.removeTags(new String[] {testImageId, anotherImageId}, tag);

        // verify
        ArgumentCaptor<String> imageIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(imageSupport, times(2)).removeTags(imageIdCapt.capture(), tag1Capt.capture());
        List<String> imageIdValues = imageIdCapt.getAllValues();
        assertArrayEquals("Image ids were not correct", new String[]{testImageId, anotherImageId},
                imageIdValues.toArray(new String[imageIdValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }
}