"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada. 
Date: 2022

About: Times the runtime of a process. 

"""

from time import time

class CalcTime:

	def __init__(self):
		pass


	def starter(self):
		"""
		Initiate the start time. 
		"""

		start_time = time()
		return start_time


	def finisher(self, start_time):
		"""
		Get the finish time. 
		"""

		end_time 	 = time()
		hours, rem   = divmod(end_time - start_time, 3600)
		minutes, sec = divmod(rem, 60)

		return (hours, minutes, sec)