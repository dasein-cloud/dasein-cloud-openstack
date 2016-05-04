package org.dasein.cloud.openstack.nova.os.compute;

import org.dasein.cloud.*;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.openstack.nova.os.*;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by mariapavlova on 12/04/2016.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest( { CinderVolume.class, Cache.class } )
public class CinderVolumeTest extends OpenStackTest {
    private NovaMethod          method;
    private CinderVolume        volumeSupport;
    private NovaOpenStack       provider;
    private AuthenticationContext authenticationContext;

    private static final String testRegionId = "testRegionId";
    private static final String testOwnerId = "5ef70662f8b34079a6eddb8da9d75fe8";
    private static final String testName = "vol-001";
    private static final String testDescription = "Another volume.";
    private static final String testVolumeId = "521752a6-acf6-4b2d-bc7a-119f9148cd8c";
    private static final String testVmId = "testVmId";
    private static final VolumeProduct fakeProduct = VolumeProduct.getInstance(testVolumeId, testName, testDescription, VolumeType.HDD);

    @Before
    public void setUp() throws Exception {
        provider = PowerMockito.mock(NovaOpenStack.class);
        try {
            PowerMockito.when(provider.isPostCactus()).thenReturn(true);
            PowerMockito.when(provider.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
            /*Class cls = NovaOpenStack.class;
            PowerMockito.when(provider.getClass()).thenReturn(cls);*/
        }
        catch( CloudException | InternalException e ) {
            e.printStackTrace();
            fail();
        }
        ProviderContext context = PowerMockito.mock(ProviderContext.class);
        PowerMockito.when(context.getRegionId()).thenReturn(testRegionId);
        PowerMockito.when(context.getAccountNumber()).thenReturn(testOwnerId);
        volumeSupport = PowerMockito.mock(CinderVolume.class);

        try {
            PowerMockito.doReturn(provider).when(volumeSupport, "getProvider");
            PowerMockito.doReturn(context).when(volumeSupport, "getContext");
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Exception occurred " + e.getMessage());
        }

        /*Cache noCache = PowerMockito.mock(Cache.class);
        PowerMockito.when(Cache.getInstance(any(CloudProvider.class), anyString(), any(Class.class), any(CacheLevel.class))).thenReturn(noCache);
        when(noCache.get(any(ProviderContext.class))).thenReturn(null);*/

        method = PowerMockito.mock(NovaMethod.class);
        try {
            whenNew(NovaMethod.class).withAnyArguments().thenReturn(method);
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Couldn't create a mock for NovaMethod construction");
        }
    }



    @Test public void testAttach() throws Exception {

    }

    @Test public void testCreateVolume() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/create_volume.json");
        when(volumeSupport.createVolume(any(VolumeCreateOptions.class))).thenCallRealMethod();
        when(method.postString(anyString(), anyString(), anyString(), any(JSONObject.class), anyBoolean()))
                .thenReturn(json);
        when(volumeSupport.listVolumeProducts()).thenReturn(Arrays.asList(fakeProduct));
        when(volumeSupport.toVolume(any(JSONObject.class), any(Iterable.class))).thenCallRealMethod();
        when(volumeSupport.getCapabilities()).thenCallRealMethod();
        VolumeCreateOptions options = VolumeCreateOptions.getInstance(new Storage<>(30, Storage.GIGABYTE), testName, testDescription);
        //run
        String volumeId = volumeSupport.createVolume(options);
        assertEquals("Returned volume id is not as expected", testVolumeId, volumeId);
    }

    @Test public void testDetach() throws Exception {
        // prepare
        Volume fakeVolume = mock(Volume.class);
        when(fakeVolume.getProviderVirtualMachineId()).thenReturn(testVmId);
        when(volumeSupport.getVolume(anyString())).thenReturn(fakeVolume);
        Mockito.doCallRealMethod().when(volumeSupport).detach(anyString(), anyBoolean());
        when(volumeSupport.getAttachmentsResource()).thenCallRealMethod();

        // run
        volumeSupport.detach(testVolumeId, false);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> suffixCapt = ArgumentCaptor.forClass(String.class);
        verify(method).deleteResource(serviceIdCapt.capture(), resourceCapt.capture(), resourceIdCapt.capture(), suffixCapt.capture());
        assertEquals("Service id is incorrect", NovaServer.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/servers", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testVmId, resourceIdCapt.getValue());
        assertEquals("Suffix is incorrect", volumeSupport.getAttachmentsResource() + "/" + testVolumeId, suffixCapt.getValue() );
    }

    @Test public void testGetCapabilities() throws Exception {
        when(volumeSupport.getCapabilities()).thenCallRealMethod();
        assertNotNull("Capabilities object cannot be null", volumeSupport.getCapabilities());
    }

    @Test public void testGetVolume() throws Exception {
        // prepare
        JSONObject json = readJson("nova/fixtures/compute/get_volume.json");
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(volumeSupport.getVolume(anyString())).thenCallRealMethod();
        when(volumeSupport.toVolume(any(JSONObject.class), any(Iterable.class))).thenCallRealMethod();
        when(volumeSupport.listVolumeProducts()).thenReturn(Arrays.asList(fakeProduct));

        // run
        Volume result = volumeSupport.getVolume(testVolumeId);

        // verify
        assertNotNull("Returned object is not expected to be null", result);
        assertEquals("Return object id is not as expected", "521752a6-acf6-4b2d-bc7a-119f9148cd8c", result.getProviderVolumeId());
    }

    @Test public void testListVolumeProducts() throws Exception {
        // prepare
        JSONObject json = readJson("nova/fixtures/compute/list_volume_types.json");
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(volumeSupport.listVolumeProducts()).thenCallRealMethod();
        when(volumeSupport.getTypesResource()).thenCallRealMethod();
        // run
        Iterable<VolumeProduct> result = volumeSupport.listVolumeProducts();

        // verify
        assertNotNull("Returned collection cannot be null", result);
        int count = 0;
        for( VolumeProduct product : result ) {
            count ++;
        }
        assertEquals("Returned number of objects is not as expected", 2, count);
    }

    @Test public void testListVolumeStatus() throws Exception {

        // prepare
        JSONObject json = readJson("nova/fixtures/compute/list_volumes.json");
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(volumeSupport.listVolumeStatus()).thenCallRealMethod();
        when(volumeSupport.toStatus(any(JSONObject.class))).thenCallRealMethod();
        // run
        Iterable<ResourceStatus> result = volumeSupport.listVolumeStatus();

        // verify
        assertNotNull("Returned collection cannot be null", result);
        int count = 0;
        for( ResourceStatus product : result ) {
            count ++;
        }
        assertEquals("Returned number of objects is not as expected", 2, count);

    }

    @Test public void testListVolumes() throws Exception {

        // prepare
        JSONObject json = readJson("nova/fixtures/compute/list_volumes.json");
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(volumeSupport.listVolumeProducts()).thenReturn(Arrays.asList(fakeProduct));
        when(volumeSupport.listVolumes()).thenCallRealMethod();
        when(volumeSupport.toVolume(any(JSONObject.class), any(Iterable.class))).thenCallRealMethod();

        // run
        Iterable<Volume> result = volumeSupport.listVolumes();

        // verify
        assertNotNull("Returned collection cannot be null", result);
        int count = 0;
        for( Volume product : result ) {
            count ++;
        }
        assertEquals("Returned number of objects is not as expected", 2, count);

    }

    @Test
    public void testIsSubscribed() throws Exception {
        AuthenticationContext context = mock(AuthenticationContext.class);
        final String testServiceUrl = "testServiceUrl";

        Mockito.when(provider.getAuthenticationContext()).thenReturn(context);
        Mockito.when(context.getServiceUrl(anyString())).thenReturn(testServiceUrl);
        when(volumeSupport.isSubscribed()).thenCallRealMethod();
        boolean test = volumeSupport.isSubscribed();
        assertTrue("Return value is incorrect", test);

    }

    @Test public void testRemove() throws Exception {
        // prepare
        Volume volume = new Volume();
        Volume deletedVolume = new Volume();
        volume.setCurrentState(VolumeState.PENDING);
        deletedVolume.setCurrentState(VolumeState.DELETED);
        volume.setProviderVolumeId("testVolumeId");

        Mockito.doCallRealMethod().when(volumeSupport).remove(anyString());
        when(volumeSupport.getVolume(anyString())).thenReturn(volume, deletedVolume);
        Mockito.doNothing().when(method).deleteResource(anyString(), anyString(), anyString(), anyString());
        // run
        volumeSupport.remove(testVolumeId);
        // verify
        ArgumentCaptor<String> volumeIdArg = ArgumentCaptor.forClass(String.class);
        verify(method).deleteResource(anyString(), anyString(), volumeIdArg.capture(), anyString());
        assertEquals("Volume Id passed to the method is not as expected", testVolumeId, volumeIdArg.getValue());

    }

    @Test
    public void testSetTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(volumeSupport).setTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        volumeSupport.setTags(testVolumeId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).createTags(serviceIdCapt.capture(), resourceCapt.capture(), resourceIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", CinderVolume.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/volumes", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testVolumeId, resourceIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );
    }

    @Test
    public void testSetTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(volumeSupport).setTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherSnapshotId = "anotherTestId";

        // run
        volumeSupport.setTags(new String[] {testVolumeId, anotherSnapshotId}, tag);

        // verify
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(volumeSupport, times(2)).setTags(resourceIdCapt.capture(), tag1Capt.capture());
        List<String> idValues = resourceIdCapt.getAllValues();
        assertArrayEquals("Resource ids were not correct", new String[]{testVolumeId, anotherSnapshotId},
                idValues.toArray(new String[idValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }

    @Test
    public void testUpdateTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(volumeSupport).updateTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        volumeSupport.updateTags(testVolumeId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).updateTags(serviceIdCapt.capture(), resourceCapt.capture(), resourceIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", CinderVolume.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/volumes", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testVolumeId, resourceIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );
    }

    @Test public void testUpdateTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(volumeSupport).updateTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherId = "anotherTestId";

        // run
        volumeSupport.updateTags(new String[] {testVolumeId, anotherId}, tag);

        // verify
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(volumeSupport, times(2)).updateTags(resourceIdCapt.capture(), tag1Capt.capture());
        List<String> imageIdValues = resourceIdCapt.getAllValues();
        assertArrayEquals("Resource ids were not correct", new String[]{testVolumeId, anotherId},
                imageIdValues.toArray(new String[imageIdValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }

    @Test
    public void testRemoveTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(volumeSupport).removeTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        volumeSupport.removeTags(testVolumeId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).removeTags(serviceIdCapt.capture(), resourceCapt.capture(), resourceIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", CinderVolume.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/volumes", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testVolumeId, resourceIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );

    }

    @Test
    public void testRemoveTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(volumeSupport).removeTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherId = "anotherTestId";

        // run
        volumeSupport.removeTags(new String[] {testVolumeId, anotherId}, tag);

        // verify
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(volumeSupport, times(2)).removeTags(resourceIdCapt.capture(), tag1Capt.capture());
        List<String> idValues = resourceIdCapt.getAllValues();
        assertArrayEquals("Resource ids were not correct", new String[]{testVolumeId, anotherId},
                idValues.toArray(new String[idValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }
}