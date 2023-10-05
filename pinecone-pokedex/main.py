import pinecone
import pandas as pd


# Load the data
df = pd.read_csv('data/pokedex.csv')

pinecone.init(      
    api_key='API KEY GOES HERE',      
    environment='gcp-starter'      
)

index = pinecone.Index('pokedex')

vectors_to_upsert = []
for _, row in df.iterrows():
    vector_id = str(row['ID'])  # Convert ID to string
    dense_values = [
        row['HP'], row['Attack'], row['Defense'], 
        row['Sp. Atk'], row['Sp. Def'], row['Speed']
    ]
    metadata = {"Name": row['Name']}
    vectors_to_upsert.append((vector_id, dense_values, metadata))


# Function to split list into chunks
def chunk_list(input_list, chunk_size):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]


# Upsert to Pinecone in chunks
for chunk in chunk_list(vectors_to_upsert, 100):
    upsert_response = index.upsert(
        vectors=chunk)