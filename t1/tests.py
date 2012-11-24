import telnetlib
from lib import insert_keys, read_keys, delete_keys


def test_insert_read(node, keyprefix="key", count=50):
    insert_keys(node, keyprefix=keyprefix, count=count)
    read_keys(node, keyprefix=keyprefix, count=count)


def test_insert_read_and_delete(node, keyprefix="key", count=50):
    insert_keys(node, keyprefix=keyprefix, count=count)
    read_keys(node, keyprefix=keyprefix, count=count)
    delete_keys(node, count, keyprefix)


def test_read(node, keyprefix="key", count=50):
    read_keys(node, keyprefix=keyprefix, count=count)

def begin_tests():
    print raw_input("Have you started memcached node(11211)??")
    node11211 = telnetlib.Telnet("localhost", 11211)
    test_insert_read(node11211, keyprefix="A")
    test_insert_read_and_delete(node11211, keyprefix="key")

    print raw_input("Have you started added another node(11212) to the cluster??\nWait for split and migrate to complete")
    node11212 = telnetlib.Telnet("localhost", 11212)
    test_read(node11211, keyprefix="A")
    test_read(node11212, keyprefix="A")

    test_insert_read(node11212, keyprefix="B")

    test_insert_read_and_delete(node11211, keyprefix="key")
    test_insert_read_and_delete(node11212, keyprefix="key")

    print raw_input("Have you started added another node(11213) to the cluster??\nWait for split and migrate to complete")
    node11213 = telnetlib.Telnet("localhost", 11213)
    test_read(node11211, keyprefix="A")
    test_read(node11212, keyprefix="A")
    test_read(node11213, keyprefix="A")
    test_read(node11211, keyprefix="B")
    test_read(node11212, keyprefix="B")
    test_read(node11213, keyprefix="B")

    test_insert_read(node11213, keyprefix="C")

    test_insert_read_and_delete(node11211, keyprefix="key")
    test_insert_read_and_delete(node11212, keyprefix="key")
    test_insert_read_and_delete(node11213, keyprefix="key")

begin_tests()


