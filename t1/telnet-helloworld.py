import telnetlib

node11211 = telnetlib.Telnet("localhost", 11211)

node11211.write("delete key1\n")
node11211.expect(["DELETED", "NOT_FOUND"])

node11211.write("get key1" + "\n")
str = node11211.read_until("END")
assert str == "\r\nEND", "key1 should not be present after deletion"

node11211.write("set key1 0 500 5" + "\n")
node11211.write("abcde" + "\r\n")
node11211.read_until("STORED")
#
node11211.write("get key1" + "\n")
node11211.read_until("\n")


