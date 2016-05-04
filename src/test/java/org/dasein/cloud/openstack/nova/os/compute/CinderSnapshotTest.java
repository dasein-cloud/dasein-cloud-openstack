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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by mariapavlova on 06/04/2016.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CinderSnapshot.class)
public class CinderSnapshotTest extends OpenStackTest {
    private NovaMethod          method;
    private Snapshot            snapshot;
    private NovaServer          vmSupport;
    private CinderSnapshot      snapshotSupport;
    private NovaComputeServices computeServices;
    private NovaOpenStack       provider;

    private String testRegionId = "testRegionId";
    private String testOwnerId = "5ef70662f8b34079a6eddb8da9d75fe8";
    private String testSnapshotId = "3fbbcccf-d058-4502-8844-6feeffdf4cb5";
    private String testName = "snap-001";
    private String testDescription = "Daily backup";
    private String testVolumeId = "testVolumeId";

    @Before
    public void setUp() throws Exception {
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

        snapshotSupport = PowerMockito.mock(CinderSnapshot.class);

        try {
            PowerMockito.doReturn(provider).when(snapshotSupport, "getProvider");
            PowerMockito.doReturn(context).when(snapshotSupport, "getContext");
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Exception occurred " + e.getMessage());
        }

        vmSupport = PowerMockito.mock(NovaServer.class);
        method = PowerMockito.mock(NovaMethod.class);
        snapshot = PowerMockito.mock(Snapshot.class);
        try {
            whenNew(NovaMethod.class).withAnyArguments().thenReturn(method);
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Couldn't create a mock for NovaMethod construction");
        }
    }


    @Test public void testCreateSnapshot() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/create_snapshot.json");
        when(snapshotSupport.createSnapshot(any(SnapshotCreateOptions.class))).thenCallRealMethod();
        when(method.postString(anyString(), anyString(), anyString(), any(JSONObject.class), anyBoolean()))
                .thenReturn(json);
        when(snapshotSupport.toSnapshot(any(JSONObject.class))).thenCallRealMethod();
        SnapshotCreateOptions options = SnapshotCreateOptions.getInstanceForCreate(testVolumeId, testName, testDescription);
        String snapshotId = snapshotSupport.createSnapshot(options);
        assertEquals("Returned snapshot id is not as expected", testSnapshotId, snapshotId);
    }

    @Test public void testGetCapabilities() throws Exception {
        when(snapshotSupport.getCapabilities()).thenCallRealMethod();
        assertNotNull("Capabilities cannot be null", snapshotSupport.getCapabilities());
    }

    @Test public void testGetSnapshot() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/get_snapshot.json");
        when(snapshotSupport.getSnapshot(anyString())).thenCallRealMethod();
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(snapshotSupport.toSnapshot(any(JSONObject.class))).thenCallRealMethod();
        Snapshot snapshot = snapshotSupport.getSnapshot(testSnapshotId);
        assertNotNull("Returned snapshot cannot be null", snapshot);
        assertEquals("Returned snapshot id is not as expected", testSnapshotId, snapshot.getProviderSnapshotId());
        assertEquals("Returned snapshot name is not as expected", testName, snapshot.getName());
        assertEquals("Returned snapshot description is not as expected", testDescription, snapshot.getDescription());
    }

    @Test public void testListSnapshotStatus() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/list_snapshot.json");
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(snapshotSupport.listSnapshotStatus()).thenCallRealMethod();
        ResourceStatus dummy = new ResourceStatus("dummy", MachineImageState.ACTIVE);
        when(snapshotSupport.toStatus(any(JSONObject.class))).thenReturn(dummy);
        //when(snapshotSupport.toSnapshot(any(JSONObject.class))).thenCallRealMethod();

        Iterator<ResourceStatus> resourceStatuses = snapshotSupport.listSnapshotStatus().iterator();
        verify(snapshotSupport, times(2)).toStatus(any(JSONObject.class));
         int count = 0;
        while( resourceStatuses.hasNext() ) {
            count++;
            resourceStatuses.next();
        }
        assertEquals("The number of returned objects is incorrect", 2, count);

    }

    @Test public void testIsPublic() throws Exception {
        when(snapshotSupport.isPublic(anyString())).thenCallRealMethod();
        assertFalse("All snapshots are private in Openstack", snapshotSupport.isPublic(testSnapshotId));
    }

    @Test public void testIsSubscribed() throws Exception {

    }

    @Test public void testListSnapshots() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/list_snapshot.json");
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(snapshotSupport.listSnapshots()).thenCallRealMethod();
        Snapshot dummy = new Snapshot();
        when(snapshotSupport.toSnapshot(any(JSONObject.class))).thenReturn(dummy);

        Iterator<Snapshot> snapshotIterator = snapshotSupport.listSnapshots().iterator();
        verify(snapshotSupport, times(2)).toSnapshot(any(JSONObject.class));
        int count = 0;
        while( snapshotIterator.hasNext() ) {
            count++;
            snapshotIterator.next();
        }
        assertEquals("The number of returned objects is incorrect", 2, count);


    }

    @Test public void testRemove() throws Exception {
        Snapshot snapshot = new Snapshot();
        Snapshot deletedSnapshot = new Snapshot();
        snapshot.setCurrentState(SnapshotState.AVAILABLE);
        deletedSnapshot.setCurrentState(SnapshotState.DELETED);
        snapshot.setProviderSnapshotId("testSnapshotId");

        Mockito.doCallRealMethod().when(snapshotSupport).remove(anyString());
        when(snapshotSupport.getSnapshot(anyString())).thenReturn(snapshot, deletedSnapshot);
        Mockito.doNothing().when(method).deleteResource(anyString(), anyString(), anyString(), anyString());

        snapshotSupport.remove(testSnapshotId);

        ArgumentCaptor<String> snapshotIdArg = ArgumentCaptor.forClass(String.class);
        verify(method).deleteResource(anyString(), anyString(), snapshotIdArg.capture(), anyString());
        assertEquals("Snapshot Id passed to the method is not as expected", testSnapshotId, snapshotIdArg.getValue());
        //        assertEquals("Current state is incorrect", MachineImageState.ACTIVE, image.getCurrentState());

    }

    @Test public void testSearchSnapshots() throws Exception {
        JSONObject json = readJson("nova/fixtures/compute/list_snapshot.json");
        when(method.getResource(anyString(), anyString(), anyString(), anyBoolean())).thenReturn(json);
        when(snapshotSupport.searchSnapshots(any(SnapshotFilterOptions.class))).thenCallRealMethod();
        Snapshot dummy = new Snapshot();
        when(snapshotSupport.toSnapshot(any(JSONObject.class))).thenReturn(dummy);

        Iterator<Snapshot> snapshotIterator = snapshotSupport.searchSnapshots(SnapshotFilterOptions.getInstance()).iterator();
        verify(snapshotSupport, times(2)).toSnapshot(any(JSONObject.class));
        int count = 0;
        while( snapshotIterator.hasNext() ) {
            count++;
            snapshotIterator.next();
        }
        assertEquals("The number of returned objects is incorrect", 2, count);
    }

    @Test
    public void testSetTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(snapshotSupport).setTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        snapshotSupport.setTags(testSnapshotId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).createTags(serviceIdCapt.capture(), resourceCapt.capture(), resourceIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", CinderSnapshot.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/snapshots", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testSnapshotId, resourceIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );
    }

    @Test
    public void testSetTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(snapshotSupport).setTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherSnapshotId = "anotherTestId";

        // run
        snapshotSupport.setTags(new String[] {testSnapshotId, anotherSnapshotId}, tag);

        // verify
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(snapshotSupport, times(2)).setTags(resourceIdCapt.capture(), tag1Capt.capture());
        List<String> idValues = resourceIdCapt.getAllValues();
        assertArrayEquals("Resource ids were not correct", new String[]{testSnapshotId, anotherSnapshotId},
                idValues.toArray(new String[idValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }

    @Test
    public void testUpdateTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(snapshotSupport).updateTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        snapshotSupport.updateTags(testSnapshotId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).updateTags(serviceIdCapt.capture(), resourceCapt.capture(), resourceIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", CinderSnapshot.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/snapshots", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testSnapshotId, resourceIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );
    }

    @Test public void testUpdateTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(snapshotSupport).updateTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherId = "anotherTestId";

        // run
        snapshotSupport.updateTags(new String[] {testSnapshotId, anotherId}, tag);

        // verify
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(snapshotSupport, times(2)).updateTags(resourceIdCapt.capture(), tag1Capt.capture());
        List<String> imageIdValues = resourceIdCapt.getAllValues();
        assertArrayEquals("Resource ids were not correct", new String[]{testSnapshotId, anotherId},
                imageIdValues.toArray(new String[imageIdValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }

    @Test
    public void testRemoveTags() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(snapshotSupport).removeTags(anyString(), any(Tag.class), any(Tag.class));
        Tag[] tags = { new Tag("a", "aa"), new Tag("b", "bb") } ;

        // run
        snapshotSupport.removeTags(testSnapshotId, tags);

        // verify
        ArgumentCaptor<String> serviceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        ArgumentCaptor<Tag> tag2Capt = ArgumentCaptor.forClass(Tag.class);
        verify(provider).removeTags(serviceIdCapt.capture(), resourceCapt.capture(), resourceIdCapt.capture(), tag1Capt.capture(), tag2Capt.capture());
        assertEquals("Service id is incorrect", CinderSnapshot.SERVICE, serviceIdCapt.getValue());
        assertEquals("Resource is incorrect", "/snapshots", resourceCapt.getValue());
        assertEquals("Resource id is incorrect", testSnapshotId, resourceIdCapt.getValue());
        assertArrayEquals("Tags are incorrect", tags, new Tag[] { tag1Capt.getValue(), tag2Capt.getValue() } );

    }

    @Test
    public void testRemoveTags1() throws Exception {
        // prepare
        Mockito.doCallRealMethod().when(snapshotSupport).removeTags(any(String[].class), any(Tag.class));
        Tag tag = new Tag("a", "aa");
        String anotherId = "anotherTestId";

        // run
        snapshotSupport.removeTags(new String[] {testSnapshotId, anotherId}, tag);

        // verify
        ArgumentCaptor<String> resourceIdCapt = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tag> tag1Capt = ArgumentCaptor.forClass(Tag.class);
        verify(snapshotSupport, times(2)).removeTags(resourceIdCapt.capture(), tag1Capt.capture());
        List<String> idValues = resourceIdCapt.getAllValues();
        assertArrayEquals("Resource ids were not correct", new String[]{testSnapshotId, anotherId},
                idValues.toArray(new String[idValues.size()]));
        assertEquals("Tag was not correct", tag, tag1Capt.getValue());
    }}