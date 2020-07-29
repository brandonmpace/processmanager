# processmanager
A multiprocessing process manager for Python programs  

It provides:  
  * Simple offloading of work to other processor cores  
  * State management for the functions in those processes  
    * It is easy to cancel in-progress work, which can be useful if a user clicks 'Cancel' in a GUI
  * Logging of exceptions while running offloaded functions  
  * Streaming of results to main process from generator functions
    * This can help smooth out IPC load curve by avoiding an all-at-once result transfer
  
Original use case:  
  * GUI program that calls back-end functions for work  
  * GUI has a cancel button that should always work, so:  
    * the GUI mainloop should not be blocked  
    * the called functions should intermittently check if the user has pressed the cancel button  
  * Want to avoid running new work when the user wants to cancel  
  * Want to work with generators and multiprocessing  
    * e.g. send results constantly to main process instead of all at once  
  
Installation:  
  * pip install processmanager  
  
Documentation:
  * [https://processmanager.readthedocs.io](https://processmanager.readthedocs.io)
  
Tested for Python >=3.6.5 on Linux (Ubuntu) and Windows 7/10  
