"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at the Education & Research Group at Esri Canada

Date: Remodified - Q2 2022

About: Sample script of how to transfer files from one VM to another. 

Requirements: 
  1) Two Azure (or AWS) VMs. 
  2) Both VMs need to communicate each other via SSH key pair (Linux). 
    - The receiver (larger VM) to have the authorized key (aka create SSH key pair). 
    - The sender (smaller VM) to have the .key file (acquire from the larger VM). 
  3) If automated, require crontab configuration (Linux). 
    
Instructions on how to set up SSH keys: 
  1) https://www.digitalocean.com/community/tutorials/how-to-set-up-ssh-keys-on-ubuntu-1804
  2) https://www.youtube.com/watch?v=Wm9N6SpAsqA
"""

import glob
import subprocess
import os 
import time 
import shutil 
from tqdm import tqdm 

csv_files = glob.glob('PATH TO CSV FILES/GTFSRT_Calgary_*.csv')

hostname   = "username@ip_address" # the receiver VM 
remotepath = "ABSOLUTE PATH IN RECEIVER VM TO STORE CSV FILES"

# Create a folder in the sender VM - this is to store csv files that have been successfully transferred 
if os.path.exists('PATH TO STORE TRANSFERRED CSV FILES IN SENDER VM'):
  os.mkdir('PATH TO STORE TRANSFERRED CSV FILES IN SENDER VM')

# Transfer CSV files over to the receiver VM.
for c in tqdm(csv_files):
  p = subprocess.Popen(['scp', '-i', 'path-in-sender-vm/name-of-.key', c, ':'.join([hostname, remotepath])])
  p.wait()
  shutil.move(c, f"PATH TO STORE TRANSFERRED CSV FILES IN SENDER VM/{c}")
