import re
import telnetlib

node11211 = telnetlib.Telnet("localhost", 11211)

def delete_key(node, key):
    node.write("delete " + key + "\n")
    #    str = node.expect(["DELETED", "NOT_FOUND"])
    str = node.read_until("\r\n")
    delete_message = re.split("\r\n", str)[0]
    assert delete_message == "DELETED" or delete_message == "NOT_FOUND", "Error while deleting key %s: Message from memcached: %s" % (
        key, delete_message)


def get_key(node, key, expected_value, expected_flag=0):
    node.write("get " + key + "\n")
    str = node.read_until("END")
    actual_metadata = re.split("\r\n", str)[1]
    actual_value = re.split("\r\n", str)[2]
    expected_metadata = "VALUE %s %d %d" % (key, expected_flag, len(expected_value))
    assert actual_metadata == expected_metadata, "Incorrect metadata"
    assert actual_value == expected_value, "GET: Expected:%s, Actual:%s" % (expected_value, actual_value)


def set_key(node, key, flag, exptime, value):
    node.write("set %s %d %d %d" % (key, flag, exptime, len(value)) + "\n")
    node.write(value + "\r\n")
    node.read_until("STORED")

print "Make sure a memcached node is running on 11211\n"

delete_key(node11211, "key1")
set_key(node11211, "key1", 0, 500, "abcde")
get_key(node11211, "key1", "abcde")


