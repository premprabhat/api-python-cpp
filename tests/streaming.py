from threading import Event
import dolphindb as ddb

conn = ddb.session()
conn.enableStreaming(7922)

def printMsg(msg):
    print(msg)

conn.subscribe("192.168.1.124", 9921, printMsg, "trades", offset=0, msgAsTable=False)
Event().wait()
