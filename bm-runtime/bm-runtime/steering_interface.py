#!/usr/bin/env python2
import argparse
import grpc
import os
import sys
import threading
import time
from time import sleep

# Import P4Runtime lib from parent utils dir
# Probably there's a better way of doing this.
sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 '../../utils/'))
import p4runtime_lib.bmv2
from p4runtime_lib.error_utils import printGrpcError
from p4runtime_lib.switch import ShutdownAllSwitchConnections
import p4runtime_lib.helper



init_time = time.time()

# TODO: interface para realizar a transicao
# colocar contadores para cada programa
# fazer um pulling dos contadores e dar um jeito de salvar eles
#  

paths_1 =  {"10.0.0.3": {"s1":[1,2,3], "s3":[1,2]}, 
          "10.0.1.1":{"s2":[2,1], "s3":[1,3]}} 

path_2 = {"10.0.0.3": [{"s1": [1,3,2]}, {"s2":[1,2]}]}

#path = {flow: sw_config[]}

def writeShadow(p4info_helper, sw_id, dst_ip_addr, nf1, nf2, nf3, port, dstAddr): 
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.shadow",
        match_fields={
            "hdr.ipv4.dstAddr": (dst_ip_addr, 32)
        },
        action_name="MyIngress.catalogue",
        action_params={
            "nf1": nf1,
            "nf2": nf2,
            "nf3": nf3,
            "port": port,
            "dstAddr": dstAddr
        })
    sw_id.WriteTableEntry(table_entry)
    print "Installed shadow rule on %s" % sw_id.name


#def write_end_to_end(p4info_helper, path):


def writeEth(p4info_helper, sw_id, dst_ip_addr, src_eth_addr, port):

    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.ipv4_lpm",
        match_fields={
            "hdr.ipv4.dstAddr": (dst_ip_addr, 32)
        },
        action_name="MyIngress.ipv4_forward",
        action_params={
            "port": port,
            "dstAddr": src_eth_addr
        })
    sw_id.WriteTableEntry(table_entry)
    print "Installed teste rule on %s" % sw_id.name


def readTableRules(p4info_helper, sw):
    """
    Reads the table entries from all tables on the switch.
    :param p4info_helper: the P4Info helper
    :param sw: the switch connection
    """

    print '\n----- Reading tables rules for %s -----' % sw.name
    for response in sw.ReadTableEntries():
        for entity in response.entities:
            entry = entity.table_entry
            # TODO For extra credit, you can use the p4info_helper to translate
            #      the IDs in the entry to names
            table_name = p4info_helper.get_tables_name(entry.table_id)
            print '%s: ' % table_name,
            for m in entry.match:
                print p4info_helper.get_match_field_name(table_name, m.field_id),
                print '%r' % (p4info_helper.get_match_field_value(m),),
            action = entry.action.action
            action_name = p4info_helper.get_actions_name(action.action_id)
            print '->', action_name,
            for p in action.params:
                print p4info_helper.get_action_param_name(action_name, p.param_id),
                print '%r' % p.value,
            print




def printCounter(p4info_helper, sw, counter_name, index, file):
    """
    Reads the specified counter at the specified index from the switch. In our
    program, the index is the tunnel ID. If the index is 0, it will return all
    values from the counter.

    :param p4info_helper: the P4Info helper
    :param sw:  the switch connection
    :param counter_name: the name of the counter from the P4 program
    :param index: the counter index (in our case, the tunnel ID)
    """
    for response in sw.ReadCounters(p4info_helper.get_counters_id(counter_name), index):
        for entity in response.entities:
            counter = entity.counter_entry
            print "%s %s %d: %d packets (%d bytes)" % (
                sw.name, counter_name, index,
                counter.data.packet_count, counter.data.byte_count
            )
            file.write(',' + str(sw.name) + "_" + str(index) + ',' + str(counter.data.byte_count))
            file.flush()
            #print(str(sw.name) + ',' + str(index) + ',' + str(counter.data.byte_count))

def storeTimestamps(p4info_helper, s1, s2, s3):
    f = open('filename.txt', "w+")

    while True:
        sleep(2)
        f.write(str(time.time() - init_time))
        printCounter(p4info_helper, s1, "MyIngress.programProcessingCounter", 1, file=f)
        printCounter(p4info_helper, s1, "MyIngress.programProcessingCounter", 2, file=f)
        printCounter(p4info_helper, s1, "MyIngress.programProcessingCounter", 3, file=f)
        printCounter(p4info_helper, s2, "MyIngress.programProcessingCounter", 1, file=f)
        printCounter(p4info_helper, s2, "MyIngress.programProcessingCounter", 2, file=f)
        printCounter(p4info_helper, s2, "MyIngress.programProcessingCounter", 3, file=f)
        printCounter(p4info_helper, s3, "MyIngress.programProcessingCounter", 1, file=f)
        printCounter(p4info_helper, s3, "MyIngress.programProcessingCounter", 2, file=f)
        printCounter(p4info_helper, s3, "MyIngress.programProcessingCounter", 3, file=f)
        f.write('\n')


def printGrpcError(e):
    print "gRPC Error:", e.details(),
    status_code = e.code()
    print "(%s)" % status_code.name,
    traceback = sys.exc_info()[2]
    print "[%s:%d]" % (traceback.tb_frame.f_code.co_filename, traceback.tb_lineno)

def main(p4info_file_path, bmv2_file_path):
    # Instantiate a P4Runtime helper from the p4info file
    p4info_helper = p4runtime_lib.helper.P4InfoHelper(p4info_file_path)

    try:
        # Create a switch connection object for s1 and s2;
        # this is backed by a P4Runtime gRPC connection.
        # Also, dump all P4Runtime messages sent to switch to given txt files.
        s1 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s1',
            address='127.0.0.1:50051',
            device_id=0,
            proto_dump_file='logs/s1-p4runtime-requests.txt')
        s2 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s2',
            address='127.0.0.1:50052',
            device_id=1,
            proto_dump_file='logs/s2-p4runtime-requests.txt')
        s3 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s3',
            address='127.0.0.1:50053',
            device_id=2,
            proto_dump_file='logs/s3-p4runtime-requests.txt')



        # Send master arbitration update message to establish this controller as
        # master (required by P4Runtime before performing any other write operation)
        s1.MasterArbitrationUpdate()
        s2.MasterArbitrationUpdate()
        s3.MasterArbitrationUpdate()            

        # Install the P4 program on the switches
        s1.SetForwardingPipelineConfig(p4info=p4info_helper.p4info,
                                       bmv2_json_file_path=bmv2_file_path)
        print "Installed P4 Program using SetForwardingPipelineConfig on s1"

        s2.SetForwardingPipelineConfig(p4info=p4info_helper.p4info,
                                       bmv2_json_file_path=bmv2_file_path)
        print "Installed P4 Program using SetForwardingPipelineConfig on s2"

        s3.SetForwardingPipelineConfig(p4info=p4info_helper.p4info,
                                       bmv2_json_file_path=bmv2_file_path)
        print "Installed P4 Program using SetForwardingPipelineConfig on s3"    


        x = threading.Thread(target=storeTimestamps, args=(p4info_helper, s1, s2, s3))
        x.start()
        
        #for i in range(1, 200):
        #    writeShadow(p4info_helper, sw_id=s1, dst_ip_addr="10.0." + str(i) + ".3", nf1=1, nf2=2, nf3=3, ttl_rounds=3, port=3)
        writeShadow(p4info_helper, sw_id=s1, dst_ip_addr="10.0.3.3", nf1=1, nf2=0, nf3=1, port=3, dstAddr="00:00:00:00:03:03")
        writeShadow(p4info_helper, sw_id=s2, dst_ip_addr="10.0.3.3", nf1=0, nf2=1, nf3=1, port=2, dstAddr="00:00:00:00:03:03")

        #writeEth(p4info_helper, sw_id=s1, dst_ip_addr="10.0.2.2", src_eth_addr="00:00:00:00:02:02" , port=2)
        #writeEth(p4info_helper, sw_id=s1, dst_ip_addr="10.0.1.1", src_eth_addr="00:00:00:00:01:01" , port=1)

        while True:
            value = input('teste:')
            print value  

        print " Shutting down."
    except grpc.RpcError as e:
        printGrpcError(e)

    ShutdownAllSwitchConnections()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='P4Runtime Controller')
    parser.add_argument('--p4info', help='p4info proto in text format from p4c',
                        type=str, action="store", required=False,
                        default='./build/basic.p4.p4info.txt')
    parser.add_argument('--bmv2-json', help='BMv2 JSON file from p4c',
                        type=str, action="store", required=False,
                        default='./build/basic.json')
    args = parser.parse_args()

    if not os.path.exists(args.p4info):
        parser.print_help()
        print "\np4info file not found: %s\nHave you run 'make'?" % args.p4info
        parser.exit(1)
    if not os.path.exists(args.bmv2_json):
        parser.print_help()
        print "\nBMv2 JSON file not found: %s\nHave you run 'make'?" % args.bmv2_json
        parser.exit(1)
    main(args.p4info, args.bmv2_json)
