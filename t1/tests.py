import telnetlib
from lib import insert_keys, read_keys, delete_keys


def test_insert_read_and_delete(node):
    insert_keys(node, 50)
    read_keys(node, 50)
    delete_keys(node, 50)


def test_read(node):
    read_keys(node, 50)

print raw_input("Have you started memcached node(11211)??")
node11211 = telnetlib.Telnet("localhost", 11211)
test_insert_read_and_delete(node11211)

print raw_input("Have you started added another node(11212) to the cluster??")
test_insert_read_and_delete(node11211)
