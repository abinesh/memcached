import telnetlib
from lib import insert_keys

node11211 = telnetlib.Telnet("localhost", 11211)
insert_keys(node11211, keyprefix="TEST", count=10000)
