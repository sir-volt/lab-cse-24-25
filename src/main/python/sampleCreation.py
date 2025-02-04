import pandas as pd
import numpy as np
import multiprocessing as mp

file_path = "../../../../datasets/big/the-reddit-covid-dataset-posts.csv"
output_path = "../../../../datasets/post_sample_data.csv"
max_sample_size = 2000
chunksize = 500_000
num_workers = mp.cpu_count() - 1

def process_chunk(chunk):
    return chunk.sample(frac=0.001, random_state=42)

def parallel_sample():
    sampled_rows = []
    with mp.Pool(processes=num_workers) as pool:
        reader = pd.read_csv(file_path, chunksize=chunksize)
        sampled_chunks = pool.map(process_chunk, reader)

    df_sampled = pd.concat(sampled_chunks, ignore_index=True).sample(n=max_sample_size, random_state=42)
    return df_sampled

if __name__ == '__main__':
    df_sampled = parallel_sample()

    df_sampled.to_csv(output_path, index=False)

    print(f"✅ Sample di {df_sampled.shape[0]:,} righe salvato in {output_path}.")
