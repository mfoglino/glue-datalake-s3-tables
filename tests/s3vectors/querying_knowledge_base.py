import time

import boto3

def query_movies_kb():
    bedrock_agent = boto3.client('bedrock-agent-runtime')

    response = bedrock_agent.retrieve(
        knowledgeBaseId='IE3VBIJWP7',
        retrievalQuery={
            'text': 'What movies were directed by Quentin Tarantino?'
            #'text': 'What movies were directed by Coppola?'
            #'text': 'Sci-fi movies'
        }
    )

    for result in response['retrievalResults']:
        print(f"")
        print(f"Metadata:{result['metadata']}")
        print(f"Content: {result['content']['text']}")
        print(f"Score: {result['score']}")
        print("---")


region = "us-east-1"
vector_bucket_name = "marcos-s3-vector"
vector_index = "marcos-vector-index"

# Create Bedrock Runtime and S3 Vectors clients in the AWS Region of your choice.
bedrock = boto3.client("bedrock-runtime", region_name=region)

def recreate_vector_index():
    s3vectors = boto3.client("s3vectors", region_name=region)
    try:
        # Try to delete the index (if method exists)
        try:
            s3vectors.delete_index(
                vectorBucketName=vector_bucket_name,
                indexName=vector_index
            )
            print("Index deleted")
            time.sleep(10)
        except:
            print("Delete index method not available, proceeding with recreation")

        # # Create new index (this might overwrite existing)
        # s3vectors.create_vector_index(
        #     vectorBucketName=vector_bucket_name,
        #     indexName=vector_index,
        #     dimensions=1024,
        #     vectorDataType="FLOAT32"
        # )
        # print("Vector index recreated successfully")

    except Exception as e:
        print(f"Error recreating index: {e}")

if __name__ == "__main__":
    #query_movies_kb()
    recreate_vector_index()