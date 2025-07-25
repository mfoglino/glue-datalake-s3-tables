# Query a vector index with an embedding from Amazon Titan Text Embeddings V2.
import boto3
import json
import time

region="us-east-1"
vector_bucket_name = "marcos-s3-vector"
vector_index = "marcos-vector-index"

# Create Bedrock Runtime and S3 Vectors clients in the AWS Region of your choice.
bedrock = boto3.client("bedrock-runtime", region_name=region)
s3vectors = boto3.client("s3vectors", region_name=region)


def main():
    # Small delay to ensure index is ready after population
    print("Waiting for index to be ready...")
    time.sleep(2)

    # Query text to convert to an embedding.
    #input_text = "clearly sci-fi movies, not drama"
    input_text = "What movies were directed by Quentin Tarantino?"

    print(f"Querying with: '{input_text}'")

    # Generate the vector embedding.
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": input_text})
    )

    # Extract embedding from response.
    model_response = json.loads(response["body"].read())
    embedding = model_response["embedding"]

    print("\n=== TOP 3 SIMILAR MOVIES ===")
    # Query vector index.
    response = s3vectors.query_vectors(
        vectorBucketName=vector_bucket_name,
        indexName=vector_index,
        queryVector={"float32": embedding},
        topK=3,
        returnDistance=True,
        returnMetadata=True
    )
    print(json.dumps(response["vectors"], indent=2))

    print("\n=== FILTERED BY SCIFI GENRE ===")
    # Query vector index with a metadata filter.
    response = s3vectors.query_vectors(
        vectorBucketName=vector_bucket_name,
        indexName=vector_index,
        queryVector={"float32": embedding},
        topK=3,
        #filter={"genre": "sci-fi"},
        filter={"genre": {"like": "drama"}},
        returnDistance=True,
        returnMetadata=True
    )
    print(json.dumps(response["vectors"], indent=2))



if __name__ == "__main__":
    main()
    