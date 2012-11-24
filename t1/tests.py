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


def normal_multinode_operations_scale_up(topology):
    print raw_input("Have you started memcached node(11211) in topology %s ??" % topology)
    node11211 = telnetlib.Telnet("localhost", 11211)
    test_insert_read(node11211, keyprefix="A")
    test_insert_read_and_delete(node11211, keyprefix="key")

    print raw_input(
        "Have you started added another node(11212) in topology %s to the cluster??\nWait for split and migrate to complete" % topology)
    node11212 = telnetlib.Telnet("localhost", 11212)
    test_read(node11211, keyprefix="A")
    test_read(node11212, keyprefix="A")

    test_insert_read(node11212, keyprefix="B")

    test_insert_read_and_delete(node11211, keyprefix="key")
    test_insert_read_and_delete(node11212, keyprefix="key")

    print raw_input(
        "Have you started added another node(11213) in topology %s to the cluster??\nWait for split and migrate to complete" % topology)
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


def normal_multinode_operations_scale_down(topology):
    node11211 = telnetlib.Telnet("localhost", 11211)
    node11212 = telnetlib.Telnet("localhost", 11212)

    print raw_input(
        "Have you removed 11213 in topology %s from the cluster??\nWait for merge and migrate to complete" % topology)
    test_read(node11211, keyprefix="A")
    test_read(node11212, keyprefix="A")

    test_insert_read(node11212, keyprefix="B")

    test_insert_read_and_delete(node11211, keyprefix="key")
    test_insert_read_and_delete(node11212, keyprefix="key")

    print raw_input(
        "Have you removed 11212 in topology %s from the cluster??\nWait for merge and migrate to complete" % topology)
    test_insert_read(node11211, keyprefix="A")
    test_insert_read_and_delete(node11211, keyprefix="key")

    pass


def print_topologies():
    print "A-11211"
    print "B-11212"
    print "C-11213"
    print "T1: A->B, B->C"
    print "T2: A->B, A->C"
    pass


def begin_tests(topologies):
    for t in topologies:
        print_topologies()
        normal_multinode_operations_scale_up(t)
        print_topologies()
        normal_multinode_operations_scale_down(t)
        print_topologies()

begin_tests(["T1", "T2"])


