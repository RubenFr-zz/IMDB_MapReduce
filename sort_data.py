import pandas as pd
import numpy as np
import time

def sort_data():
	titles_db = pd.read_csv('basic.tsv', delimiter="\t", index_col='tconst')
	casts_db = pd.read_csv('principals.tsv', delimiter="\t")
	actors_db = pd.read_csv('names.tsv', delimiter="\t", index_col='nconst')

	titles = titles_db.originalTitle
	actors = actors_db.primaryName
	casts = casts_db[(casts_db["category"] == 'actor') | (casts_db["category"] == 'actress')][['tconst', 'nconst']]

	casts['nconst'] = casts['nconst'].map(actors)
	casts['tconst'] = casts['tconst'].map(titles)

	casts = casts.groupby(['tconst'])['nconst'].apply(lambda x: ','.join(x.astype(str))).reset_index()[['tconst', 'nconst']]

	casts.to_csv('sorted_data.csv', sep ='\t', index = False)

if __name__ == '__main__':
	print('Starting Sorting Data Set...')
	tic = time.perf_counter()
	sort_data()
	toc = time.perf_counter()
	print(f"Sorted Data Set in {((toc - tic)/60):0.4f} minutes")
