# import os
# import sys
# import servicemanager
# import win32event
# import win32service
# import win32serviceutil

# class MyService(win32serviceutil.ServiceFramework):
#     _svc_name_ = "PythonService"
#     _svc_display_name_ = "Python Custom Service"
#     _svc_description_ = "This is a test Python Windows Service."

#     def __init__(self, args):
#         win32serviceutil.ServiceFramework.__init__(self, args)
#         self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

#     def SvcStop(self):
#         self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
#         win32event.SetEvent(self.hWaitStop)

#     def SvcDoRun(self):
#         servicemanager.LogMsg(
#             servicemanager.EVENTLOG_INFORMATION_TYPE,
#             servicemanager.PYS_SERVICE_STARTED,
#             (self._svc_name_, "")
#         )
#         win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

# if __name__ == '__main__':
#     win32serviceutil.HandleCommandLine(MyService)



import os
import sys
import servicemanager
import win32event
import win32service
import win32serviceutil
import threading




# Ensure the main alchemy_v9 script can be found
SCRIPT_PATH = r"c:\Users\muham.CRUX_NIVAS\Desktop\my_works\python\testpython\alchemy_v9.py"  # Update this path
sys.path.append(os.path.dirname(SCRIPT_PATH))

# print(f'"Python executable: {sys.executable}"')
# print(f"Python path: {sys.path}")

from alchemy_v9 import main  # Import main function

class MyService(win32serviceutil.ServiceFramework):
    _svc_name_ = "AlchemyV9Service"
    _svc_display_name_ = "Alchemy V9 Data Processor"
    _svc_description_ = "Processes database operations based on external API requests."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.thread = None
        self.running = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.running = False
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, "")
        )

        self.thread = threading.Thread(target=self.run_main_loop)
        self.thread.start()
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

    def run_main_loop(self):
        while self.running:
            try:
                main()  # Call the main function from alchemy_v9
            except Exception as e:
                servicemanager.LogErrorMsg(f"Error: {str(e)}")
            finally:
                import time
                time.sleep(60)  # Wait before retrying to avoid excessive API calls

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(MyService)
