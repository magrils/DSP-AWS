package Common;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class EC2Handler {

    private Ec2Client ec2;
    private Region region;

//    private final String img = "ami-0ed9277fb7eb570c9";   // t.a's custom ami: amazon linux


    private final String img = "ami-0cff7528ff583bf9a";   // standard ami: amazon linux
//    private final String img = "ami-03eff83120eec1198";   // my ami: amazon linux

    private final String baseUserData =
                    "#!/bin/bash\n" +
                    "set -x\n" +
                    "mkdir task1\n" +
                    "cd task1\n" +
                    "mkdir logs\n" +
                    "echo \"$PWD\"\n" +
                    "sudo chmod 777 .\n" +
                    "sudo yum -y install java-11-amazon-corretto\n";
    private final String workerUserData =
        baseUserData +
        "sudo aws s3 cp s3://task1ec2bucket/Worker.jar worker.jar\n"+
        "sudo aws s3 cp s3://task1ec2bucket/englishPCFG.ser.gz model/englishPCFG.ser.gz\n"+
//            "sudo wget https://task1ec2bucket.s3.amazonaws.com/Worker.jar -O worker.jar\n" +
//            "sudo wget https://task1ec2bucket.s3.amazonaws.com/englishPCFG.ser.gz -O model\\englishPCFG.ser.gz\n" +
        "java -jar worker.jar";

    private final String managerUserData =
        baseUserData +
        "sudo aws s3 cp s3://task1ec2bucket/Manager.jar manager.jar\n"+
//        "sudo wget https://task1ec2bucket.s3.amazonaws.com/Manager.jar -O manager.jar\n" +
        "java -jar manager.jar";

    public EC2Handler() {
//        region = Region.US_WEST_2;
        region = Region.US_EAST_1;
        this.ec2 = Ec2Client.builder().region(region).build();
    }

    public void close(){
        ec2.close();
    }

    public List<String> runInstance(String roleTagVal, String timeout, int units){
        System.out.println("Requesting new "+roleTagVal+" instance");


        String encUserData = null;
        if(Role.worker.equals(roleTagVal)){
            encUserData = getEncodedUserData(workerUserData);
        }
        else{
            encUserData = getEncodedUserData(managerUserData);
        }
        // Iam roles
        IamInstanceProfileSpecification iamInstanceProfile = IamInstanceProfileSpecification.builder()
                .name("LabInstanceProfile")
                .build();


        Tag tag = Tag.builder()
                .key(Role.Role)
                .value(roleTagVal)
                .build();

        try {

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(img)
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(units)
                    .minCount(1)
                    .keyName("vockey")
                    .userData(encUserData)
                    .securityGroupIds("sg-041a4ad69b81d478f")
                    .iamInstanceProfile(iamInstanceProfile)
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId();
            List<String> instanceIds = new ArrayList<>();
            for (Instance ins : response.instances()) {
                instanceIds.add(ins.instanceId());
            }

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            ec2.createTags(tagRequest);

            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s\n",
                    instanceId, img);

            return instanceIds;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return new ArrayList<>();
    }


    private String getEncodedUserData(String userData){
        String base64UserData = null;
        try {
            base64UserData = new String( Base64.getEncoder().encode(userData.getBytes( "UTF-8" )), "UTF-8" );
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(5);
        }
        return base64UserData;
    }


    public void findRunningEC2Instances() {

        try {
            String nextToken = null;

            do {
                Filter filter = Filter.builder()
                        .name("instance-state-name")
                        .values("running")
                        .build();

                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(filter)
                        .build();

                DescribeInstancesResponse response = ec2.describeInstances(request);
                System.out.println("# of reservations : "+response.reservations().size());
                for (Reservation reservation : response.reservations()) {
                    System.out.println("# of instances : "+reservation.instances().size());
                    for (Instance instance : reservation.instances()) {
                        System.out.printf(
                                "Found Reservation with id %s, " +
                                        "AMI %s, " +
                                        "type %s, " +
                                        "state %s " +
                                        "and monitoring state %s",
                                instance.instanceId(),
                                instance.imageId(),
                                instance.instanceType(),
                                instance.state().name(),
                                instance.monitoring().state());
                        System.out.println("");
                    }
                }
                nextToken = response.nextToken();

            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


    public String findManagersUp() {
        try {
            String nextToken = null;

            do {
                Filter filter = Filter.builder()
                        .name("instance-state-name")
                        .values("running", "pending")
                        .build();

                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(filter)
                        .build();

                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        List<Tag> tags = instance.tags();
                        for (Tag tag : tags) {
                            if (tag.key().equals(Role.Role) && tag.value().equals(Role.manager)){
                                return instance.instanceId();
                            }
//                            System.out.printf("tag: %s , value: %s\n",tag.key(),tag.value());
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            return null;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void terminateEC2(String instanceID) {

        try{
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceID)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();

            for (int i = 0; i < list.size(); i++) {
                InstanceStateChange sc = (list.get(i));
                System.out.println("The ID of the terminated instance is "+sc.instanceId());
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }
    // snippet-end:[ec2.java2.terminate_instance]

    public List<TagDescription> getInstanceTags(String instanceId) {

        try {
            Filter filter = Filter.builder()
                    .name("resource-id")
                    .values(instanceId)
                    .build();

            DescribeTagsResponse describeTagsResponse = ec2.describeTags(DescribeTagsRequest.builder().filters(filter).build());
            List<TagDescription> tags = describeTagsResponse.tags();
            return tags;
        } catch ( Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void describeTags(String instanceId) {

        try {

            Filter filter = Filter.builder()
                    .name("resource-id")
                    .values(instanceId)
                    .build();

            DescribeTagsResponse describeTagsResponse = ec2.describeTags(DescribeTagsRequest.builder().filters(filter).build());
            List<TagDescription> tags = describeTagsResponse.tags();
            for (TagDescription tag: tags) {
                System.out.println("Tag key is: "+tag.key());
                System.out.println("Tag value is: "+tag.value());
            }

        } catch ( Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}

