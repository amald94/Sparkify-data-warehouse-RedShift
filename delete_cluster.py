import configparser
import boto3


def delete_redshift_cluster():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    try:
        print('Deleting redshift cluster...')

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')
        DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
        redshift = boto3.client('redshift',
                                region_name='us-west-2',
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET)

        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)


delete_redshift_cluster()