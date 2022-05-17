"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada
Date: Q4 - 2021

About: Perform parallel processing. 

"""

from multiprocessing import Process, Pool, set_start_method, cpu_count
from tqdm import tqdm 

class ParallelProcess:
	
	def __init__(self, start_method, targeter, parameters, split_val):
		"""
		Use the Process function in multiprocessing to parallel process. 

		:params start_method: The method to initiate - typically in Linux -> "fork"; Windows -> "spawn". 
		:params targeter: The custom function that is to be parallel processed. 
		:params parameters: The parameters required in the custom function. 
		:params split_val: The list that is to be split into chunks. 
		"""

		self._process(start_method=start_method, 
					  targeter=targeter, 
					  parameters=parameters, 
					  split_val=split_val)


	def _process(self, start_method, targeter, parameters, split_val):
		"""
		Initiate parallel processing. 

		:params start_method: The method to initiate - typically in Linux -> "fork"; Windows -> "spawn".
		:params targeter: The custom function that is to be parallel processed. 
		:params parameters: The parameters required in the custom function. 
		:params split_val: The list that is to be split into chunks. 
		"""

		set_start_method(method=start_method, force=True)
		processes = []
		for i in range(len(split_val)):
			new_param = (split_val[i], ) + parameters[1:]
			p = Process(target = targeter, args = new_param)
			processes.append(p)
			p.start()
		for process in processes:
			process.join()


class ParallelPool:

	def __init__(self, start_method, partial_func, main_list):
		"""
		Use the Pool function in multiprocessing to parallel process. 

		:params start_method: The method to initiate - typically in Linux -> "fork"; Windows -> "spawn".
		:params partial_func: A custom partial function that takes most of the parameters of a custom function to be parallel processed.
		:params main_list: A numpy array list that has been chunked into n number of cores.
		"""

		self._pool(start_method=start_method, partial_func=partial_func, main_list=main_list)


	def _pool(self, start_method, partial_func, main_list):
		"""
		Initiate parallel processing. 

		:params start_method: The method to initiate - typically in Linux -> "fork"; Windows -> "spawn".
		:params partial_func: A custom partial function that takes most of the parameters of a custom function to be parallel processed.
		:params main_list: A numpy array list that has been chunked into n number of cores. 
		"""

		set_start_method(method=start_method, force=True)
		with Pool(processes=cpu_count()) as p:
			max_ = len(main_list)
			with tqdm(total=max_) as pbar:
				for i, _ in enumerate(p.imap_unordered(partial_func, main_list)):
					pbar.update()
			p.close()
			p.join()