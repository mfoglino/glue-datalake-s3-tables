import boto3

def query_movies_kb():
    bedrock_agent = boto3.client('bedrock-agent-runtime')

    response = bedrock_agent.retrieve(
        knowledgeBaseId='IE3VBIJWP7',
        retrievalQuery={
            #'text': 'What movies were directed by Quentin Tarantino?'
            'text': 'What movies were directed by Coppola?'
            #'text': 'Sci-fi movies'
        }
    )

    for result in response['retrievalResults']:
        print(f"")
        print(f"Metadata:{result['metadata']}")
        print(f"Content: {result['content']['text']}")
        print(f"Score: {result['score']}")
        print("---")

if __name__ == "__main__":
    query_movies_kb()