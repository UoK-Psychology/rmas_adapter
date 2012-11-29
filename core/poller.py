'''
    This module handles the polling of the RMAS ESB
'''


from threading import Timer

import datetime
import logging
from core.rmas_bus import RMASBus
from conf import settings


last_poll = None
bus = None

def start_polling():
    '''
        This function starts the poller polling, it will poll immediately
        and then repeat the poll after the settings.POLL_INTERVAL. When a
        event is received from the bus, it will see if the event is in the 
        settings.EVENTS list and call the appropriate event handler
    '''
    global bus, last_poll
    last_poll = datetime.datetime.now()
    bus = RMASBus()
    poll_for_events()
    
        
    

def poll_for_events():
    logging.info('Polling ESB')
    
    global last_poll
    events = bus.get_events(last_poll.isoformat())
    last_poll = datetime.datetime.now()
    
    for event in events:
        #handle the events as described in the settings.EVENTS list
        logging.info('Got event: %s' % event)
    #update the last_poll time - we only want new events after this time.

    Timer(settings.POLL_INTERVAL/1000, poll_for_events).start()#poll again in 2 seconds time!

