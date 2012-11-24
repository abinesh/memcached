import telnetlib
from lib import insert_keys, read_keys, delete_keys


def test_insert_read_and_delete(node, keyprefix="key"):
    insert_keys(node, keyprefix=keyprefix, count=50)
    read_keys(node, keyprefix=keyprefix, count=50)
    delete_keys(node, 50)


def test_read(node, keyprefix="key"):
    read_keys(node, keyprefix=keyprefix, count=50)

node11211 = telnetlib.Telnet("localhost", 11211)

print raw_input("Have you started memcached node(11211)??")
test_insert_read_and_delete(node11211,keyprefix = "a")

print raw_input("Have you started added another node(11212) to the cluster??")
test_insert_read_and_delete(node11211,keyprefix="b")
