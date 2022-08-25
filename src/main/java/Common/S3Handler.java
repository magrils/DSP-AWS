package Common;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

public class S3Handler {
    private S3Client s3;
    private Region region;

    public S3Handler() {
        region = Region.US_WEST_2;
//        region = Region.US_EAST_1;
        this.s3 = S3Client.builder().region(region).build();
    }


    public void close(){
        s3.close();
    }

    public void createBucket( String bucketName) {

        System.out.println("S3 - Creating bucket '"+bucketName+"':");
        S3Waiter s3Waiter = s3.waiter();

        try {
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(region.id())
                                    .build())
                    .build();

            s3.createBucket(bucketRequest);

            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println("S3 - "+bucketName +" is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(5);
        }

        System.out.println("Done");
    }

    public void putObject(String bucketName, String key, File file){

        System.out.println("S3 - Putting Object '"+key+"' in bucket '"+bucketName+"':");
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try {
            s3.putObject(objectRequest, RequestBody.fromFile(file));
//        check response from s3. put object
        } catch (SdkClientException ex) {
            ex.printStackTrace();
            System.out.println("something wrong with Client");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.out.println("something wrong with S3 connection");
        }

        System.out.println("Done");
    }

    public void getObjectBytes(String bucketName, String keyName, File file) {

        System.out.println("S3 - Getting Object '"+keyName+"' from bucket '"+bucketName+"':");
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file
            OutputStream os = new FileOutputStream(file);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }

        System.out.println("Done");
    }

    public void listObjects(String bucketName){
        System.out.println("S3 - listing Objects in bucket '"+bucketName+"':");
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(10)
                .build();
        try{
            ListObjectsV2Iterable listRes = s3.listObjectsV2Paginator(listReq);

            // Process response pages
            listRes.contents().stream()
                    .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }

        System.out.println("Done");
    }

    public void deleteObject(String bucketName, String key){
        System.out.println("S3 - Deleting Object '"+key+"' from bucket '"+bucketName+"':");
        // Delete an object
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try{
            s3.deleteObject(deleteObjectRequest);
        }
        catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.out.println("something wrong with S3 connection");
        }
        System.out.println("Done");
    }

    public void deleteBucket(String bucket) {
        System.out.println("S3 - Deleting bucket '"+bucket+"'!");
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucket)
                .build();
        try {

            s3.deleteBucket(deleteBucketRequest);
        }
        catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
        System.out.println("Done");
    }

    public String getObjectUrl(String bucket, String key){
        try {
            String url = s3.utilities().getUrl(builder -> builder.bucket(bucket).key(key)).toExternalForm();
            return url;
        }
        catch (SdkException e) {
            e.printStackTrace();
            System.exit(5);
        }
        return null;
    }

}
