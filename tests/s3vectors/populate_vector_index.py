# Populate a vector index with embeddings from Amazon Titan Text Embeddings V2.
import boto3
import json

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from tests.s3vectors.generate_movies import generate_movies

region = "us-east-1"
vector_bucket_name = "marcos-s3-vector"
vector_index = "marcos-vector-index"

# Create Bedrock Runtime and S3 Vectors clients in the AWS Region of your choice.
bedrock = boto3.client("bedrock-runtime", region_name=region)
s3vectors = boto3.client("s3vectors", region_name=region)


def main():
    # Texts to convert to embeddings.
    # texts = [
    #     "Star Wars: A farm boy joins rebels to fight an evil empire in space",
    #     "Jurassic Park: Scientists create dinosaurs in a theme park that goes wrong",
    #     "Finding Nemo: A father fish searches the ocean to find his lost son"
    # ]
    movies_data = list(generate_movies())
    texts = [movie['text'] for movie in movies_data]

    # Generate vector embeddings.
    embeddings = []
    for text in texts:
        print(f"Proccesing: {text[:10]}")
        response = bedrock.invoke_model(
            modelId="amazon.titan-embed-text-v2:0",
            body=json.dumps({"inputText": text})
        )

        # Extract embedding from response.
        response_body = json.loads(response["body"].read())
        embeddings.append(response_body["embedding"])

    # Write embeddings into vector index with metadata.
    vectors = []
    for i, movie in enumerate(movies_data):

        if i == 5:
            print(f"Emb: {embeddings[i]}")
            print(f"Emb length: {len(embeddings[i])}")
        vectors.append({
            "key": f"{movie['title']} ({movie['year']})",
            "data": {"float32": embeddings[i]},
            "metadata": {
                "source_text": movie['text'],
                "genre": movie['genre'].split(',')[0].strip().lower(),
                "director": movie['director'],
                "year": str(movie['year']),
                "imdb_score": str(movie['imdb_score'])
            }
        })

    s3vectors.put_vectors(
        vectorBucketName=vector_bucket_name,
        indexName=vector_index,
        vectors=vectors
    )


if __name__ == "__main__":
    main()
