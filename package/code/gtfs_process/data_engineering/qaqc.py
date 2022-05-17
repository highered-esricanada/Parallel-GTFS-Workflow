"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada
Date: Remodified Q1 - 2022

About: Improves the data quality by omitting values that seem out-of-place (duplicates and not in trending order).
"""

from pandas import DataFrame


class QaQc:

	def __init__(self, df:DataFrame, unique_val, L2, raw_date, folder_date, output_folder):
		"""
		Improves the data quality by omitting out-of-place values.

		:param df: The dataframe containing extracted geographic information of each vehicle location per transit route.
		:param unique_val: The value of the transit route being assessed.
		:param L2: The list that is part of the Manager in Multiprocessing.
		:param raw_date: The date of the raw GTFS-RT. 
		:param folder_date: The folder that belongs to the static GTFS update across the project directory specifically that aligns with the date of the GTFS-RT.

		:return: Cleaner dataframe
		"""

		self.clean_df = self._improve_data(df=df, L2=L2,
		                                   raw_date=raw_date,
		                                   unique_val=unique_val,
		                                   folder_date=folder_date,
		                                   output_folder=output_folder)


	def _check_diffs(self, e: DataFrame, order):
		"""
		Occasionally in GIS, an observation will hit a segment that overlaps another segment. The
		question is which one to keep? This checks difference by degree order and omits rows that
		have a negative difference; thus, indicating out-of-place values of the trending order.

		:param e: A subset dataframe that is being assessed - group by trip_id.
		:param order: Degree of difference.

		:return: Cleaner subset dataframe.
		"""

		return (
			e
				.assign(diff     = lambda d: d['stop_seque'].diff(order), # Difference by stop sequence
			            idx_diff = lambda d: d['index'].diff(order))      # Difference by segment index value
				.query('diff >= 0 or diff.isnull() or idx_diff >= 0 or idx_diff.isnull()', engine='python')
				.drop(columns=['diff', 'idx_diff'])
		)


	def _improve_data(self, df: DataFrame, unique_val, L2, raw_date, folder_date, output_folder):
		"""
		Improves the data quality by omitting out-of-place observations.

		:param df: The dataframe containing extracted geographic information of each vehicle location per transit route.
		:param unique_val: The value of the transit route being assessed.
		:param L2: The list that is part of the Manager in Multiprocessing.
		:param raw_date: The date of the raw GTFS-RT. 
		:param folder_date: The folder that belongs to the static GTFS update across the project directory specifically that aligns with the date of the GTFS-RT.
		:param output_folder: Contents to be exported and stored in a specific folder.

		:return: Cleaner dataframe
		"""

		# Drop unnecessary duplicate values. - Original
		semi_df = (
			df
				.sort_values(['trip_id', 'Local_Time', 'barcode'])
				.assign(dupl = lambda e: e['trip_id'].astype(str) + "-" + e['Local_Time'].astype(str) + "-" + e['index'].astype(str))
				.drop_duplicates('dupl') # Reduce unnecessary observations
				.drop(columns=['dupl'])
		)

		# Omit negative values that seem to be out-of-placed.
		fin_df = (
			semi_df
				.groupby(['trip_id'], as_index=False)
				.apply(lambda e: self._check_diffs(e, 3)) # 3rd degree difference
				.groupby(['trip_id'], as_index=False)
				.apply(lambda e: self._check_diffs(e, 2)) # 2nd degree difference
				.groupby(['trip_id'], as_index=False)
				.apply(lambda e: self._check_diffs(e, 1)) # 1st degree difference
		)

		ori_length = semi_df.shape[0]
		fin_length = fin_df.shape[0]

		retained = round((fin_length / ori_length)*100, 2)

		L2.append(f"{unique_val},{raw_date},{folder_date},{retained}")

		clean_name = f"{output_folder}/{raw_date}_{unique_val}_cleaned.csv"

		fin_df.to_csv(clean_name, index=False)

		return fin_df
