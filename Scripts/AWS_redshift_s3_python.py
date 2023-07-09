#!/usr/bin/env python
# coding: utf-8

# In[112]:


#In this tutorial, I will show all operation required to create cluster and tables.

# Make sure install boto3,pg8000,psycopg2 ,pandas using pip
# Make sure have access key and secret key 

# we are createing subnet group,security group ,tables in redshift and also upload data from s3 into redshift cluster
#clean the system drop tables ,subnet goup and security group


# In[ ]:


import boto3
import json
#import psycopg2

access_key = 'AKIXXZ5XXXXABXXXXXXX'
secret_key = 'LFsN3L8TFg/5jbXXXXHG+u/XXXXXXXXXXX'

vpc_id='vpc-63XXXX08'


# In[113]:


#create security group by passing vpc_id and group name
from botocore.exceptions import ClientError

#create security group
ec2_client = boto3.client('ec2', 
                          aws_access_key_id=access_key, 
                          aws_secret_access_key=secret_key)

group_name = 'my-redshift-security-group'
group_description = 'Security group for Redshift cluster access'




try:
    # Create the security group
    response = ec2_client.create_security_group(
        GroupName=group_name,
        Description=group_description,
        VpcId=vpc_id
    )
    security_group_id = response['GroupId']
    print('Created security group with ID:', security_group_id)
except ClientError as e:
    if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
        # The security group already exists
        response = ec2_client.describe_security_groups(
            Filters=[
                {'Name': 'group-name', 'Values': [group_name]},
                {'Name': 'vpc-id', 'Values': [vpc_id]}
            ]
        )
        security_group_id = response['SecurityGroups'][0]['GroupId']
        print('Security group already exists. Using existing security group with ID:', security_group_id)
    else:
        # Handle other exceptions
        print('Error creating security group:', e)




#sg-08f82fe187f58b3ab


# In[114]:


#create inbound rule for security group

import boto3
from botocore.exceptions import ClientError



# Create an EC2 client
ec2_client = boto3.client('ec2', 
                          aws_access_key_id=access_key, 
                          aws_secret_access_key=secret_key)

#security_group_id sg-08f82fe187f58b3ab  retun by above code
print(security_group_id)  


port = 5439
ip_range = '0.0.0.0/0'

try:
    # Add the inbound rule to the security group
    response = ec2_client.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': port,
                'ToPort': port,
                'IpRanges': [{'CidrIp': ip_range}]
            }
        ]
    )
    print('Inbound rule added to the security group.')
except ClientError as e:
    if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
        print('Inbound rule already exists for the specified port and IP range.')
    else:
        print('Error adding inbound rule:', e)


# In[115]:


# check vpc and subnet


ec2_client = boto3.client('ec2', 
                          aws_access_key_id=access_key, 
                          aws_secret_access_key=secret_key)

# Fetch and print subnet details
response = ec2_client.describe_subnets()

for subnet in response['Subnets']:
    subnet_id = subnet['SubnetId']
    vpc_id = subnet['VpcId']
    cidr_block = subnet['CidrBlock']
    availability_zone = subnet['AvailabilityZone']

    print(f"Subnet ID: {subnet_id}")
    print(f"VPC ID: {vpc_id}")
    print(f"CIDR Block: {cidr_block}")
    print(f"Availability Zone: {availability_zone}")
    print("---")


# In[76]:


# The create_cluster_subnet_group operation in Amazon Redshift is used to create a subnet group that 
# represents a group of subnets. When creating a Redshift cluster, 
# you are required to associate the cluster with a subnet group.

# Deploying a cluster in multiple subnets allows you to distribute the cluster across 
# different availability zones (AZs). 

# If you have data sources or clients in different regions or AZs, placing the cluster in multiple subnets closer 
# to those data sources or clients can help reduce data transfer costs.


# In[116]:


#Need to create subnet group which conatins list of subnet

import boto3
from botocore.exceptions import ClientError


# Create a Redshift client
redshift_client = boto3.client('redshift', 
                               aws_access_key_id=access_key, 
                               aws_secret_access_key=secret_key
                              )

subnet_group_name = 'my-subnet-group'
subnet_ids = ['subnet-e4129a9f',
              'subnet-100e4f5c',
              'subnet-1237277a'
             ]   # Replace with the appropriate subnet IDs

try:
    # Create the subnet group
    response = redshift_client.create_cluster_subnet_group(
        ClusterSubnetGroupName=subnet_group_name,
        Description='My subnet group for redshift description',
        SubnetIds=subnet_ids
    )
    print(subnet_group_name)
    print('Subnet group created successfully.')
except ClientError as e:
    if e.response['Error']['Code'] == 'ClusterSubnetGroupAlreadyExists':
        print('Subnet group already exists. Skipping creation.')
    else:
        print('Error creating subnet group:', e)


# In[117]:


# Define the cluster parameters used to create cluster in redshift
cluster_parameters = {
    'ClusterIdentifier': 'my-redshift-cluster',
    'NodeType': 'dc2.large',
    'MasterUsername': 'myawsuser',
    'MasterUserPassword': 'Password13',
    'DBName': 'mydatabase',
    'ClusterType': 'single-node',
    'NumberOfNodes': 1,
    'PubliclyAccessible': True,
    'VpcSecurityGroupIds': [security_group_id],  # you take from above we already create security group
    'AvailabilityZone': 'ap-south-1a', # primarily created in the specified availability zone.
    'Port': 5439,
    'ClusterSubnetGroupName': subnet_group_name    #created above wih name my-subnet-group
     
    # Add any other necessary cluster parameters here
}


# In[118]:


#finally create cluster in redshift by passing cluster parameter

import boto3


# Create a Redshift client
redshift_client = boto3.client('redshift', 
                               aws_access_key_id=access_key, 
                               aws_secret_access_key=secret_key)



# Create the cluster
try:
    response = redshift_client.create_cluster(**cluster_parameters)
    print('Redshift cluster creation initiated.')
except redshift_client.exceptions.ClusterAlreadyExistsFault:
    print('Cluster already exists. Skipping cluster creation.')
    # You can choose to exit the program or perform other actions as needed
    # exit()





#ClusterIdentifier parameter specifies the unique identifier for your Redshift cluster.
#The redshift_client.get_waiter('cluster_available').wait() statement waits until the Redshift cluster becomes available. 
#By default, it will continuously check the cluster status until it becomes available 
# Wait for the cluster to be available

redshift_client.get_waiter('cluster_available').wait(
    ClusterIdentifier=cluster_parameters['ClusterIdentifier']
)

print('Redshift cluster is now available.')


# In[119]:


#I already created mynewredshiftfortest this role and have permission for s3 access to redshift.

iam=boto3.client('iam',
                  region_name='ap-south-1',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)

roleArn=iam.get_role(RoleName='mynewredshiftfortest')['Role']['Arn']
print(roleArn)


# In[120]:


#modify_cluster_iam_roles method is used to modify the IAM roles associated with an Amazon Redshift cluster.
s3_access_role_arn = roleArn

redshift_client = boto3.client('redshift', 
                               aws_access_key_id=access_key, 
                               aws_secret_access_key=secret_key)

redshift_client.modify_cluster_iam_roles(
    ClusterIdentifier=cluster_parameters['ClusterIdentifier'],
    AddIamRoles=[s3_access_role_arn]
)

print('These roles will be granted permissions to access s3 within the cluster.')


# In[121]:


#describe tthe cluster value
# retrieves information about a specific Redshift cluster identified by the ClusterIdentifier my-redshift-cluster

redshift_client = boto3.client('redshift', 
                               aws_access_key_id=access_key, 
                               aws_secret_access_key=secret_key)

cluster_info =redshift_client.describe_clusters(ClusterIdentifier='my-redshift-cluster')['Clusters'][0]
print(cluster_info)


# In[122]:


#pg8000 and psycopg2 are both Python libraries used for interacting with PostgreSQL databases, including Amazon Redshift. 
#pg8000 is a lightweight and pure-Python PostgreSQL adapter that aims for simplicity and ease of use.
#psycopg2 is known for its performance and is often the preferred choice for high-performance database interactions.

import pg8000




redshift_endpoint = 'my-redshift-cluster.cjoyi9o6hfqt.ap-south-1.redshift.amazonaws.com'
redshift_port = 5439
redshift_user = 'myawsuser'
redshift_password = 'Password13'
redshift_database = 'mydatabase'
redshift_table = 'product_table'

# Create a connection to Redshift using pg8000
conn = pg8000.connect(host=redshift_endpoint,
                      port=redshift_port,
                      database=redshift_database,
                      user=redshift_user,
                      password=redshift_password)

cursor = conn.cursor()

# Create the table if it does not exist
create_table_command = """
CREATE TABLE IF NOT EXISTS product_table (
marketplace varchar(50),
customer_id varchar(50),
product_id varchar(50),
seller_id varchar(50),
sell_date varchar(50),
quantity integer
);
"""


try:
    # Execute the create table command
    cursor.execute(create_table_command)
    conn.commit()
    print('product_table table created successfully or already exists.')
except pg8000.Error as e:
    print('Error creating table:', e)

# Close the cursor and connection
cursor.close()
conn.close()




# In[123]:


#insert data into table using copy command
#copy data from s3 bucket into redshift table
#s3://mypythonproject1/input/product_data.csv  copy data into redshift table product_table


import pg8000


# Create a connection to Redshift
#execute a copy command in cluster . to copy data from s3://mypythonproject1/input/product_data.csv into redshift table
conn = pg8000.connect(
    host=redshift_endpoint,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password
)

# Create a cursor to execute SQL statements
cursor = conn.cursor()


input_bucket = 'mypythonproject1'
input_file_key = 'input/product_data.csv'



copy_command = f"""
COPY public.product_table
FROM 's3://mypythonproject1/input/product_data.csv'
CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
DELIMITER ',' IGNOREHEADER 1;
"""


try:
    # Execute the COPY command
    cursor.execute(copy_command)
    conn.commit()
    print('COPY command executed successfully.')
except pg8000.Error as e:
    print('Error executing COPY command:', e)

# Close the cursor and connection
cursor.close()
conn.close()


# In[124]:


#psycopg2 is known for its performance and is often the preferred choice for high-performance database interactions.
#create table emp in redshift using psycopg2



import psycopg2




redshift_endpoint = 'my-redshift-cluster.cjoyi9o6hfqt.ap-south-1.redshift.amazonaws.com'
redshift_port = 5439
redshift_user = 'myawsuser'
redshift_password = 'Password13'
redshift_database = 'mydatabase'
redshift_table = 'product_table'

# Create a connection to Redshift using pg8000
conn = psycopg2.connect(host=redshift_endpoint,
                      port=redshift_port,
                      database=redshift_database,
                      user=redshift_user,
                      password=redshift_password)

cursor = conn.cursor()

# Create the table if it does not exist
create_table_command = """
CREATE TABLE IF NOT EXISTS emp (
emp_id int,
name varchar(100),
salary decimal

);
"""



try:
    # Execute the create table command
    cursor.execute(create_table_command)
    conn.commit()
    print('emp table created successfully or already exists.')
except psycopg2.Error as e:
    print('Error creating table:', e)

# Close the cursor and connection
cursor.close()
conn.close()


# In[125]:


#copy emp txt file into table emp in redshift using psycopg2


import psycopg2


# Create a connection to Redshift
#execute a copy command in cluster . to copy data from s3://mypythonproject1/input/product_data.csv into redshift table
conn = psycopg2.connect(
    host=redshift_endpoint,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password
)

# Create a cursor to execute SQL statements
cursor = conn.cursor()


input_bucket = 'myglue-etl-project'
input_file_key = 'input/emp.txt'



copy_command = f"""
COPY public.emp
FROM 's3://mypythonproject1/input/emp.txt'
CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
DELIMITER ',' IGNOREHEADER 1;
"""


try:
    # Execute the COPY command
    cursor.execute(copy_command)
    conn.commit()
    print('COPY command executed successfully.')
except pg8000.Error as e:
    print('Error executing COPY command:', e)

# Close the cursor and connection
cursor.close()
conn.close()

print('COPY command executed successfully.')


# In[126]:


#check error

import psycopg2
import pandas as pd

# Connect to the Redshift cluster
conn = psycopg2.connect(
    host=redshift_endpoint,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password
)

# Create a cursor
cur = conn.cursor()

# Execute the SQL query to retrieve load error details from 'stl_load_errors'
cur.execute("SELECT * FROM stl_load_errors")

# Fetch all the rows returned by the query
load_errors = cur.fetchall()

#Print the load error details
for error in load_errors:
    print(error)

# Close the cursor and connection
cur.close()
conn.close()


#format is not good 


# Create a dataframe from the load_errors data so convert into dataframe
df = pd.DataFrame(load_errors, columns=[desc[0] for desc in cur.description])

# Display the dataframe
display(df)



# In[127]:


#Now correct DELIMITER and again execute code
#we need to use pipe delemiter

#copy emp txt file into table emp in redshift using psycopg2


import psycopg2


# Create a connection to Redshift
#execute a copy command in cluster . to copy data from s3://mypythonproject1/input/product_data.csv into redshift table
conn = psycopg2.connect(
    host=redshift_endpoint,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password
)

# Create a cursor to execute SQL statements
cursor = conn.cursor()


input_bucket = 'myglue-etl-project'
input_file_key = 'input/emp.txt'



copy_command = f"""
COPY public.emp
FROM 's3://mypythonproject1/input/emp.txt'
CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
DELIMITER '|' IGNOREHEADER 1;
"""


try:
    # Execute the COPY command
    cursor.execute(copy_command)
    conn.commit()
    print('COPY command executed successfully.')
except pg8000.Error as e:
    print('Error executing COPY command:', e)

# Close the cursor and connection
cursor.close()
conn.close()


# In[128]:


#now select data of emp and product from redshift 
#query into emp and product_table in reshift

import psycopg2

# Connect to the Redshift cluster
conn = psycopg2.connect(
    host=redshift_endpoint,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password
)

# Create a cursor
cur = conn.cursor()

# Execute the SELECT query
cur.execute("SELECT * FROM emp")

# Fetch all the rows returned by the query
rows = cur.fetchall()

# Process the retrieved rows
for row in rows:
    print(row)

# Close the cursor and connection
cur.close()
conn.close()


# In[131]:


#now select data of emp and product from redshift 
#query into emp and product_table in reshift

import psycopg2

# Connect to the Redshift cluster
conn = psycopg2.connect(
    host=redshift_endpoint,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password
)

# Create a cursor
cur = conn.cursor()

# Execute the SELECT query
cur.execute("SELECT * FROM product_table")

# Fetch all the rows returned by the query
rows = cur.fetchall()

# Process the retrieved rows
for row in rows:
    print(row)

# Close the cursor and connection
cur.close()
conn.close()


# In[130]:


#Now drop Mutiple table at once 
#drop table emp and product_table in reshift
import psycopg2

# Connect to the Redshift cluster
conn = psycopg2.connect(
    host=redshift_endpoint,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password
)

# Create a cursor
cur = conn.cursor()

# List of tables to drop
tables_to_drop = ['emp', 'product_table']

# Drop the tables
for table_name in tables_to_drop:
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

print('Drop table successfully.')    
# Commit the changes to the database
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()


# In[132]:


#once you done delete your redshift cluster
import boto3


# Create a Redshift client
redshift_client = boto3.client('redshift', 
                               aws_access_key_id=access_key, 
                               aws_secret_access_key=secret_key)

cluster_identifier = 'my-redshift-cluster'

# Delete the Redshift cluster
redshift_client.delete_cluster(ClusterIdentifier=cluster_identifier,
                              SkipFinalClusterSnapshot=True)


redshift_client.get_waiter('cluster_deleted').wait(ClusterIdentifier=cluster_identifier)

print("delete cluster successfully")


# In[133]:


#Delete the subnet group 'my-subnet-group'  which you created above:
#subnet_group_name = 'my-subnet-group'

import boto3


# Create a Redshift client
redshift_client = boto3.client('redshift', 
                               aws_access_key_id=access_key, 
                               aws_secret_access_key=secret_key)






# Delete the subnet group
redshift_client.delete_cluster_subnet_group(ClusterSubnetGroupName=subnet_group_name)


#A status code of 200 indicates that the request was successful. 


# In[134]:


#delete security group  security_group_id  which you created above


ec2_client = boto3.client('ec2', 
                          aws_access_key_id=access_key, 
                          aws_secret_access_key=secret_key)

# Delete the security group
ec2_client.delete_security_group(GroupId=security_group_id)


#'HTTPStatusCode' field in the response. If the value is 200, it indicates a successful 


# In[ ]:




