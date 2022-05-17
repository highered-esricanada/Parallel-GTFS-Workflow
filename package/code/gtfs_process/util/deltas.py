"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada.
Date: Created Q1-2022

About: 2 Classes with their own unique purpose:
		a) TimeDelta - uses NumPy to identify the time delta (changes in time) in seconds.
		b) SpatialDelta - uses the ArcGIS API for Python to construct Polyline geometry and calculate length.
"""

from numpy import datetime64, timedelta64
from arcgis.geometry import Polyline


class TimeDelta:

	def __init__(self, start, end):
		"""
		Get the time delta - aka the difference between two timestamps.

		:param start: The start time - formatted: 'YYYY-mm-dd HH:MM:SS'
		:param end: The end time - formatted: 'YYYY-mm-dd HH:MM:SS'
		"""

		self.change_time = self._time_delta(start=start, end=end)

	def _time_delta(self, start: str, end: str, unit='s'):
		"""
		:param start: The start time - formatted: 'YYYY-mm-dd HH:MM:SS'
		:param end: The end time - formatted: 'YYYY-mm-dd HH:MM:SS'
		:param unit: The unit in seconds of time delta.

		:return: Time delta in seconds.
		"""

		try:
			delta = (datetime64(end) - datetime64(start)).astype(f"timedelta64[{unit}]")
			return int(delta / timedelta64(1, 's'))

		except Exception as e:
			return None


class SpatialDelta:

	def __init__(self, paths, wkid):
		"""
		Construct Polyline geometry paths and get the length of it.

		:param paths: Nested list of lists in proper format containing coordinate pairs to construct Polyline.
		:param wkid: The spatial reference number to project the geometry Polyline.
		"""

		self.dist = self._pth_dist(paths=paths, wkid=wkid)


	def _pth_dist(self, paths, wkid):
		"""
		:param paths: Nested list of lists in proper format containing coordinate pairs to construct Polyline.
		:param wkid: The spatial reference number to project the geometry Polyline.

		:return: Distance value in meters.
		"""

		line = {'paths': paths, 'spatialReference': {'wkid': wkid}}

		poly_path = Polyline(line)
		distance = round(poly_path.get_length("PRESERVE_SHAPE", "METERS"), 2)

		return distance