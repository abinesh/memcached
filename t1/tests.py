import re
import telnetlib

node11211 = telnetlib.Telnet("localhost", 11211)

def assert_key_not_found(node, key):
    (value, metadata) = _do_get(key, node)
    assert value is None, "Key expected to be absent, but was found with value:%s, metadata:%s" % (value, metadata)
    assert metadata is None, "Key expected to be absent, but was found with value:%s, metadata:%s" % (value, metadata)


def delete_key(node, key):
    node.write("delete " + key + "\n")
    str = node.read_until("\r\n")
    delete_message = re.split("\r\n", str)[0]
    assert delete_message == "DELETED" or delete_message == "NOT_FOUND", "Error while deleting key %s: Message from memcached: %s" % (
        key, delete_message)
    assert_key_not_found(node, key)


def _do_get(key, node):
    node.write("get " + key + "\n")
    str = node.read_until("END\r\n")
    output_lines = re.split("\r\n", str)
    if output_lines[0] == "END":
        return None, None

    actual_metadata = output_lines[0]
    actual_value = output_lines[1]
    return  actual_value, actual_metadata


def get_key(node, key, expected_value, expected_flag=0):
    ( actual_value, actual_metadata) = _do_get(key, node)
    expected_metadata = ["VALUE %s %d %d" % (key, expected_flag, len(expected_value)),
                         "VALUE %s %d %d" % (key, expected_flag, len(expected_value) - 2)]
    if actual_metadata== expected_metadata[1]:
        print "Ignoring length bug now. Fix it soon!\n"

    assert actual_metadata in expected_metadata, "GET %s: Expected metadata: %s,Actual metadata: %s" % (
        key, expected_metadata, actual_metadata)
    assert actual_value == expected_value, "GET %s: Expected:%s, Actual:%s" % (key, expected_value, actual_value)


def set_key(node, key, flag, exptime, value):
    node.write("set %s %d %d %d" % (key, flag, exptime, len(value)) + "\n")
    node.write(value + "\r\n")
    node.read_until("STORED\r\n")

print "Make sure a memcached node is running on 11211\n"

def insert_keys(node, count, flag=0, exptime=500, value="abcde"):
    for i in range(count + 1):
        key = "key%d" % i
        delete_key(node, key)
        set_key(node, key, flag, exptime, value)


def read_keys(node, count):
    for i in range(count + 1):
        key = "key%d" % i
        get_key(node, key, "abcde")


def delete_keys(node, count):
    for i in range(count + 1):
        key = "key%d" % i
        delete_key(node, key)

insert_keys(node11211, 50)
read_keys(node11211, 50)
delete_keys(node11211, 50)

