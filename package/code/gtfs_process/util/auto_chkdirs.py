"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research Group at Esri Canada.

Date: Q2 - 2022.

About:  For the AutoMake Class:
	    Makes a directory folder if it does not exist. Ideally, folder to store byproducts during processes.

Warning: Update the variable: "geo_census_dir" if there are changes to the geo census levels (additions or removals).
"""

import os


class AutoMake:

	def __init__(self, storage_folder):
		"""
		:param storage_folder: The folder that will be created.
		"""

		self._auto_create(storage_folder=storage_folder)

	def _auto_create(self, storage_folder):
		"""
		:param storage_folder: The folder that will be created.

		:return: None.
		"""
		[os.makedirs(storage_folder) if not os.path.isdir(storage_folder) else storage_folder]
