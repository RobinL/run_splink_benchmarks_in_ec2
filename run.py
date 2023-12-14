import boto3
from datetime import datetime

def create_and_upload_file(bucket_name, file_name, content):
    # Create a file with the specified content
    with open(file_name, 'w') as file:
        file.write(content)

    # Initialize the S3 client
    s3_client = boto3.client('s3')

    # Upload the file to S3
    s3_client.upload_file(file_name, bucket_name, file_name)
    print(f"File '{file_name}' uploaded to bucket '{bucket_name}'.")

if __name__ == "__main__":
    bucket = "robinsplinkbenchmarks"
    
    # Get current datetime and format it as a string
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Append the datetime to the file name
    file_name = f"hello_world_{current_time}.txt"
    content = "hi"

    create_and_upload_file(bucket, file_name, content)
