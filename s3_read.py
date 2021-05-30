import boto3

region = 'ap-southeast-2'
s3 = boto3.resource('s3', region).meta.client
ssm = boto3.client('ssm', region)
bucket_name = ssm.get_parameter(Name='d_data_bucket_name', WithDecryption=False)['Parameter']['Value']
file_name = ssm.get_parameter(Name='d_data_file_name', WithDecryption=False)['Parameter']['Value']

def list_details_from_file():
    
    expression = "select * from s3object s"
    
    result = s3.select_object_content(
            Bucket=bucket_name,
            Key=file_name,
            ExpressionType='SQL',
            Expression=expression,
            InputSerialization={'JSON':{'Type':'Document'}},
            OutputSerialization={'JSON':{}}
    )
    for payload in result['Payload']:
        if "Records" in payload:
            print(payload['Records']['Payload'].decode('utf-8'))


list_details_from_file()

