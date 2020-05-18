import boto3
import pandas as pd
from botocore.exceptions import ClientError
import configparser
import json
import psycopg2

### Create IAM Role and Attach Policy to it
def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    # create an IAM role for redshift 
    # so that other aws services like s3 can be
    # accessed from redshift cluster
    
    try:
        # create IAM role
        print("1. Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
        # Attach poliy to give s3 read access
        print("2. Attaching Policy")
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn=("arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
                            )['ResponseMetadata']['HTTPStatusCode']
        
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn=("arn:aws:iam::aws:policy/AmazonRedshiftFullAccess")
                                      )['ResponseMetadata']['HTTPStatusCode']
                               
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn=("arn:aws:iam::aws:policy/AdministratorAccess")
                               )['ResponseMetadata']['HTTPStatusCode']
                               
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        print(roleArn)
        return roleArn
    except Exception as e:
        print(e)

def createClients(KEY,SECRET,IAM_ROLE_NAME):
    print("creating client")
    ec2 = boto3.resource('ec2',
                     region_name='us-west-2', 
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

    s3 = boto3.resource('s3',
                         region_name='us-west-2', 
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    iam = boto3.client('iam',
                         region_name='us-west-2', 
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    redshift = boto3.client('redshift',
                         region_name='us-west-2', 
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    
    roleArn = create_iam_role(iam, IAM_ROLE_NAME)
    print(roleArn)
    return redshift,roleArn, ec2, s3,roleArn

### Create Redshift Cluster
def create_cluster(redshift, roleArn, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, 
                   DB_NAME, CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD):
    print("Creating cluster")
    try:
        redshift.create_cluster(        
            #HW
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
        
def openPort(redshift,CLUSTER_IDENTIFIER,ec2,PORT):
    # get the cluster properties to open port 
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    ENDPOINT = myClusterProps['Endpoint']['Address']
    ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(PORT),
            ToPort=int(PORT)
        )
        
        return ENDPOINT,ROLE_ARN
    except Exception as e:
        print(e)
        return ENDPOINT,ROLE_ARN

def main():
    # read the config file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    CLUSTER_TYPE       = config.get("CLUSTER","CLUSTER_TYPE")
    NUM_NODES          = config.get("CLUSTER","NUM_NODES")
    NODE_TYPE          = config.get("CLUSTER","NODE_TYPE")

    CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
    DB_NAME                 = config.get("CLUSTER","DB_NAME")
    DB_USER            = config.get("CLUSTER","DB_USER")
    DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    PORT               = config.get("CLUSTER","DB_PORT")

    IAM_ROLE_NAME      = config.get("IAM_ROLE", "ARN")

    ### 1. Create Clients
    redshift,roleArn,ec2, s3,roleArn = createClients(KEY,SECRET,IAM_ROLE_NAME)
    print(roleArn)
    ### 2. Create the cluster
    create_cluster(redshift, roleArn, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, 
               DB_NAME, CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD)
#     ### 3. Get End points
    ENDPOINT,ROLE_ARN = openPort(redshift,CLUSTER_IDENTIFIER,ec2,PORT)
    print(ENDPOINT)
    print(ROLE_ARN)
    ### 3. Check the connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(ENDPOINT,DB_NAME,DB_USER,DB_PASSWORD,PORT))
    cur = conn.cursor()
    if cur:
        print('Connected')
    conn.close()


if __name__ == "__main__":
    main()
    
    