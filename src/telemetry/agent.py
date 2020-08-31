from abc import ABC, abstractmethod
import threading
import losantmqtt as losant



# this is an abstract clas. Each db implementation has to implement these three methods
class TelemetryAgent(ABC):

    @abstractmethod
    def on_command(self, device, command) -> bool:
        pass

    @abstractmethod
    def send_state(self, name, value) -> bool:
        pass

    @abstractmethod
    def set_callback(self, callback) -> bool:
        pass



class LosantAgent(TelemetryAgent, ABC, threading.Thread):

    def __init__( self, my_device_id, my_app_access_key, my_app_access_secret ):
        super().__init__()
        threading.Thread.__init__(self)

        self.my_device_id         = my_device_id
        self.my_app_access_key    = my_app_access_key
        self.my_app_access_secret = my_app_access_secret

        # Construct Losant device
        self.device = losant.Device( self.my_device_id,
                        self.my_app_access_key,
                        self.my_app_access_secret)

        print("Losant Device Set")

        self.callback = None


    def run( self ):
        # Connect to Losant and leave the connection open
        print("Losant Device Start")
        self.device.add_event_observer("command", self.on_command)
        self.device.connect(blocking=True)


    def send_state(self, name, value):
        print("Sending Device State")
        self.device.send_state( { str(name) : value } )


    def on_command(self, device, command):
        print(command["name"] + " command received.")

        if command["name"] == "toggle":
            self.callback(command)
            print("Do something")

    def set_callback(self, callback):
        self.callback = callback
        print("Losant Callback set")
