package org.dasein.cloud.openstack.nova.os.storage;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.openstack.nova.os.*;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.util.uom.storage.Byte;
import org.dasein.util.uom.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 * Created by mariapavlova on 19/04/2016.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest( { SwiftBlobStore.class } )
/*


 */
public class SwiftBlobStoreTest extends OpenStackTest {
    private     SwiftMethod           method;
    private     SwiftBlobStore        swiftBlobSupport;
    private     NovaOpenStack         provider;
    private     Blob                  blobMock;
    private     AuthenticationContext authenticationContext;

    private static final String        testRegionId    = "testRegionId";
    private static final String        testOwnerId     = "5ef70662f8b34079a6eddb8da9d75fe8";
    private static final String        testBucketName  = "testBucketName";
    private static final String        testBucketLocation  = "testBucketLocation";
    private static final String        testObjectName = "testObjectName";
    private static final String        testStorageUrl = "testStorageUrl";

    @Before
    public void setUp() throws Exception {
        provider = mock(NovaOpenStack.class);
        try {
            PowerMockito.when(provider.isPostCactus()).thenReturn(true);
            PowerMockito.when(provider.getCloudProvider()).thenReturn(OpenStackProvider.OTHER);
        }
        catch( CloudException | InternalException e ) {
            e.printStackTrace();
            fail();
        }
        ProviderContext context = mock(ProviderContext.class);
        PowerMockito.when(context.getRegionId()).thenReturn(testRegionId);
        PowerMockito.when(context.getAccountNumber()).thenReturn(testOwnerId);
        swiftBlobSupport = mock(SwiftBlobStore.class);

        try {
            PowerMockito.doReturn(provider).when(swiftBlobSupport, "getProvider");
            PowerMockito.doReturn(context).when(swiftBlobSupport, "getContext");
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Exception occurred " + e.getMessage());
        }
        // auth context
        authenticationContext = Mockito.mock(AuthenticationContext.class);
        Mockito.when(provider.getAuthenticationContext()).thenReturn(authenticationContext);
        Mockito.when(authenticationContext.getStorageUrl()).thenReturn(testStorageUrl);

        /*Cache noCache = PowerMockito.mock(Cache.class);
        PowerMockito.when(Cache.getInstance(any(CloudProvider.class), anyString(), any(Class.class), any(CacheLevel.class))).thenReturn(noCache);
        when(noCache.get(any(ProviderContext.class))).thenReturn(null);*/

        method = mock(SwiftMethod.class);
        try {
            whenNew(SwiftMethod.class).withAnyArguments().thenReturn(method);
        }
        catch( Exception e ) {
            e.printStackTrace();
            fail("Couldn't create a mock for NovaMethod construction");
        }
    }


    @Test public void testGetCapabilities() throws Exception {

    }

    @Test public void testCreateBucket() throws Exception {
        // prepare
        Blob blobMock = mock(Blob.class);
        when(swiftBlobSupport.getBucket(anyString())).thenReturn(blobMock);
        when(swiftBlobSupport.exists(anyString())).thenReturn(true);
        when(swiftBlobSupport.findFreeName(anyString())).thenReturn(testBucketName);
        Mockito.doNothing().when(swiftBlobSupport).createBucket(anyString());
        when(swiftBlobSupport.createBucket(anyString(), any(Boolean.class))).thenCallRealMethod();
        // run
        Blob bucket = swiftBlobSupport.createBucket(testBucketName, true);
        // verify
        assertEquals("Returned bucket name is not as expected", blobMock, bucket);

    }

    @Test public void testExists() throws Exception {

    }

    @Test public void testGetBucket() throws Exception {
        // prepare
        Blob blobMock = mock(Blob.class);
        when(swiftBlobSupport.getBucket(anyString())).thenCallRealMethod();
        when(blobMock.isContainer()).thenReturn(true);
        when(swiftBlobSupport.list(anyString())).thenReturn(Arrays.asList(blobMock));
        when(blobMock.getBucketName()).thenReturn(testBucketName);
        // run
        Blob bucket = swiftBlobSupport.getBucket(testBucketName);
        //verify
        assertEquals("Returned bucket name is not as expected ", blobMock, bucket);
    }

    @Test public void testGetObject() throws Exception {
        // prepare
        Blob blobMock = mock(Blob.class);
        when(swiftBlobSupport.list(anyString())).thenReturn(Arrays.asList(blobMock));
        when(blobMock.getObjectName()).thenReturn(testObjectName);
        when(swiftBlobSupport.getObject(anyString(), anyString())).thenCallRealMethod();
        // run
        Blob bucket = swiftBlobSupport.getObject(testBucketName, testObjectName);
        //verify
        assertEquals("Returned Object is not as expected", blobMock, bucket);
    }

    @Test public void testGetSignedObjectUrl() throws Exception {

    }

    @Test public void testGetObjectSize() throws Exception {
        // prepare
        Blob blobMock = mock(Blob.class);

        Map<String, String> map = new HashMap<>();
        map.put(testBucketName, testObjectName);
        when(method.head(anyString(), anyString())).thenReturn(map);
        when(swiftBlobSupport.getMetaDataLength(any(Map.class))).thenReturn(100L);
//        when(method, "getContext").thenReturn(context);
        when(swiftBlobSupport.getObjectSize(anyString(), anyString())).thenCallRealMethod();
        //run
        Storage<Byte> res = swiftBlobSupport.getObjectSize(testBucketName, testObjectName);
        //verify
        assertEquals("Returned object size is not as expected", 100L, res.longValue());

    }

    @Test public void testGet() throws Exception {
        // prepare
        File tempFile = File.createTempFile("dasein", "blob-get");
        tempFile.deleteOnExit();
        FileTransfer fileTransferMock = mock(FileTransfer.class);
        final String testContent = "testContent";
        InputStream stream = new ByteArrayInputStream(testContent.getBytes());

        when(swiftBlobSupport.exists(anyString())).thenReturn(false);
        when(method.get(anyString(), anyString())).thenReturn(stream);
        Mockito.doCallRealMethod().when(swiftBlobSupport).get(anyString(), anyString(), any(File.class), any(FileTransfer.class));
        PowerMockito.doCallRealMethod().when(swiftBlobSupport, "copy", any(InputStream.class), any(OutputStream.class), any(FileTransfer.class));

        //run
        swiftBlobSupport.get(testBucketName, testBucketLocation, tempFile, fileTransferMock);

        //verify
        String resultContent = new String(Files.readAllBytes(tempFile.toPath()));
        assertEquals("Resulting copied file content is not as expected", testContent, resultContent);
    }

    @Test public void testIsPublic() throws Exception {

    }

    @Test public void testIsSubscribed() throws Exception {

    }

    @Test public void testList() throws Exception {

    }

    @Test public void testMakePublic() throws Exception {

    }

    @Test public void testMakePublic1() throws Exception {

    }

    @Test public void testMapServiceAction() throws Exception {

    }

    @Test public void testMove() throws Exception {

    }

    @Test public void testPut() throws Exception {

    }

    @Test public void testPut1() throws Exception {

    }

    @Test public void testRemoveBucket() throws Exception {

    }

    @Test public void testRemoveObject() throws Exception {

    }

    @Test public void testRenameBucket() throws Exception {

    }

    @Test public void testRenameObject() throws Exception {

    }

    @Test public void testUpload() throws Exception {

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