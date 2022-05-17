"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada. 

Modified: 2022

About: Finds documents in a defined directory including all sub-folders - returns as a DataFrame. 
"""

import os
from pandas import DataFrame, to_datetime
from numpy import where 

class DiscoverDocs:

    def __init__(self):
        pass


    def __call__(self, path: str) -> DataFrame:
        """
        Validate the path and get path details (sub-folders, files)
        
        :params path: str => Path to location of interest. 
        """
        
        self._validate(path)                    # Check the parameter
        self._path = path 
        return self._walk_directory(self._path) # Get details about documents in folder & subfolders. 


    @staticmethod
    def _validate(path):
        """
        Checks that the supplied path is a string and does exist. 

        :params path: str => Path to location of interest. 
        """

        if not isinstance(path, str):
            raise AttributeError(f"Parameter is not a string: {str(path)}")

        elif not os.path.exists(path):
            error_msg = (
                f"Path does not exist: {str(path)}\n"
                f"Alternatively, check if the path has restrictions have been applied to execute os.stat if the path does not exist."
            )

            raise AttributeError(error_msg)

    @staticmethod
    def _get_human_readable_format_storage_size(size, decimal_places = 3):

        storage_size = {"byte":     "B",
                        "kilobyte": "KB",
                        "megabyte": "MB",
                        "gigabyte": "GB",
                        "terabyte": "TB",
                        "petabyte": "PB",
                        "exabyte": "EB",
                        "zettabyte": "ZB",
                        "yottabyte": "YB"}

        for unit in storage_size.values():
            if size < 1024.0:
                break
            else: 
                size /= 1024.0

        return f"{size:.{decimal_places}f}{unit}" 

    def _get_document_details(self, wd: DataFrame) -> DataFrame:
        """
        Acquire document details (e.g., abspath, ctime, mtime, size, etc.)

        :params wd: The work directory as a dataframe. 

        TODO: Implement creation (ctime) and modified (mtime) times. These are platform dependent and 
              if not done properly can produce unexpected outcomes (e.g. mtime displaying as timestamp 
              occuring before ctime). 

              More details on this issue can be found here: https://stackoverflow.com/questions/237079/how-to-get-file-creation-modification-date-times
        """

        # Some documents metadata is not accessible by the OS module (highly unlikey), trace these instances to avoid crashing the process (e.g. retrieving "ctime" won't work for those instances)
        details_qc = {
            "accessible":  lambda r: r['path'].apply(lambda t: os.path.exists(t)).astype(int),
            "is_document": lambda r: r['path'].apply(lambda t: os.path.isfile(t)).astype(int)
        }

        details_doc = {
            "abspath"  : lambda r: r['path'].apply(lambda t: os.path.abspath(t)),                               # Get the absolute path (instead of the default relative path in the walk_directory method)
            # "ctime"    : lambda r: r['path'].apply(lambda t: os.path.getctime(t)).pipe(to_datetime, unit='s'),  # Get the creation time (convert from Unix Epoch to human readable)
            # "mtime"    : lambda r: r['path'].apply(lambda t: os.path.getmtime(t)).pipe(to_datetime, unit='s'),  # Get the modified time (convert from Unix Epoch to human readable)
            "size"     : lambda r: r['path'].apply(lambda t: os.path.getsize(t)),                               # Get the size of the document (bytes by default, future feature may use parameter to specify GB, TB, human readable, etc)
            "size_hrf" : lambda r: r["size"].apply(self._get_human_readable_format_storage_size) 
        }

        order_cols = ['abspath', 'path', 'directory', 'filename',
                      # 'ctime', 'mtime',
                      'size', 'size_hrf']

        return (
            wd
                .assign(**details_qc)
                # Fetch metadata for those files whose metadata is not restricted
                .pipe(lambda d: 
                    d
                        .query('is_document == 1 and accessible == 1')
                        .assign(**details_doc)
                        .append(d.query('is_document == 0 or accessible == 0'))
                )
                [order_cols]
        )


    def _walk_directory(self, path: str) -> DataFrame:
        """
        List files in the provided folder, subfolder, filenames. 

        :params path: The string path.

        :returns: Final DataFrame that provides path, directory, and filename. 
        """

        list_of_files_only = [(folder, filename) 
                              for folder, subfolders, filenames in os.walk(path)
                              for filename in filenames]

        details_path = {
            "dir"       : lambda r: r['dir'].replace("\\\\", "/"),
            "directory" : lambda r: r['dir'] + r['dir'].str.endswith('/').pipe(where, "", "/"),
            "path"      : lambda r: r['directory'] + r['filename']
        }

        return (
            DataFrame(list_of_files_only, columns=['dir', 'filename'])
                .assign(**details_path)
                [['path', 'directory', 'filename']]
                # Get metadata about the document (i.e., creation, modification, etc)
                .pipe(self._get_document_details)
        )


def discover_docs(path):
    return DiscoverDocs()(path)