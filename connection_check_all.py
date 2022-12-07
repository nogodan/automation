#########################################################################
# Synopsis
# This script fetches VPN information from the local VPN VM
# which includes tcpdump info to check IPsec origin ip address. 
# Also collect OLB IP address from zk info then test it with wget/curl/ping cmd
# to validate L3/L4/L7 lvl reachability
# Taeho Choi taeho.choi@nutanix.com                 Jan/27/2021 
# Added ENG-355137                                  Feb/9/2021 
# Added NET-3204                                    Feb/9/2021 
# Added check for static route customer -  NET-3479 Feb/11/2021
# Added logic for direct connection customer        Feb/11/2021
# Added logic to detect envoy service on PC         May/15/2021
# Updated logging module for logz.io                July/5/2021
#########################################################################

import os 
import sys
import json
from datetime import datetime

if os.path.isdir("/usr/local/nutanix"):
    sys.path.insert(0, "/usr/local/nutanix")
    sys.path.insert(0, "/usr/local/nutanix/bin")
    import env

    from gevent import monkey
    monkey.patch_all()
      
    from aplos.lib.nusights.base_heartbeat import BaseHeartbeat
    from aplos.lib.remote_connection.connection_heartbeat import ConnectionHeartbeat
    from aplos.lib.remote_connection import remote_connection_api
    from aplos.lib.remote_connection.remote_connection_api import RemoteConnectionApi
    from aplos.shell.utils.remote_connection_utils import get_all_remote_connections
    from aplos import interfaces 
    from aplos.interfaces import AplosInterfaces
    from aplos.client.intentengine.lib.common import invoke_local_rest_api
    import ConfigParser
    from aplos.sl_bufs.xi_mgmt_config_pb2 import XiPcConfig
    from aplos.lib.utils.xi_config_utils import XiConfigUtil    
    from util.interfaces.interfaces import NutanixInterfaces
    import util.zookeeper.zookeeper_interface as zookeeper_interface
    import cluster.genesis_utils as genesis_utils
    import subprocess
    import re
    import numpy as np

cwd = os.getcwd()
if os.path.isdir(cwd):
    sys.path.insert(2, cwd)
import common_utils as utils
    
interfaces = NutanixInterfaces()
zk_session = interfaces.zk_session

#Function to collect VPN detail from aplos interface
def get_vpn_detail(err,respdict):
    #Collect response as json format
    utils.log.info("## There is a total {} VPN GW(s)".format(len(respdict["entities"])))
    vpnName=[ i["spec"]["name"] for i in respdict["entities"]]
    maxfield = len(max(vpnName,key=len))
    utils.log.info(50*"#")
    for i in respdict["entities"]:
        utils.log.info("GW NAME: " + i["spec"]["name"].ljust(maxfield+2)+"VER: " + str(i["status"]["resources"]["deployment"].get("installed_software_version")).ljust(22) + "PUBLIC_IP: " + str(i["status"]["resources"].get("public_ip")).ljust(18) + "TYPE:" + i["status"]["resources"]["gateway_type"].ljust(8) + "STATUS: " + str(i["status"]["resources"]["operational_status"].get("state")))
    utils.log.info(50*"#")

#Function for local VPN public ip, will be used to run VPN internal cmd
def get_local_vpn_pub_ip(err,respdict):
    local_vpn_ip=[]
    for i in respdict["entities"]:
        if i["status"]["resources"]["gateway_type"] == "LOCAL":
            pub_ip = i["status"]["resources"].get("public_ip")
            utils.log.info("Local VPN Name: {0} has a public ip: {1}".format(i["spec"]["name"],pub_ip))
            local_vpn_ip.append(pub_ip)
    return local_vpn_ip

#Function for remote VPN public ip, will be used to compare VPN peer ip address
def get_remote_vpn_pub_ip(err,respdict):
    remote_vpn_ip=[]
    utils.log.info(50*"#")
    for i in respdict["entities"]:
        if i["status"]["resources"]["gateway_type"] == "REMOTE":
            pub_ip = i["status"]["resources"].get("public_ip")
            utils.log.info("Remote VPN Name: {0} has a public ip: {1}".format(i["spec"]["name"],pub_ip))
            remote_vpn_ip.append(str(pub_ip))
    return remote_vpn_ip

#Function to collect XLB configuration - OLB IP address
def get_xi_olb_ip():
    proto = XiConfigUtil.get_xi_mgmt_config_from_zk(zk_session)
    if proto:
        xlb_address = getattr(proto, "xlb_virtual_address")
        olb_address = getattr(proto, "olb_virtual_address")
        #only return olb_ip_address 
        #detail https://sourcegraph.canaveral-corp.us-west-2.aws/5.15.2-main@HEAD/-/blob/aplos/py/aplos/intentgw/v3_pc/api/base_physical_availability_zone_resource.py#L244
    return olb_address.ip

def search_string_in_file(file_name, string_to_search):
    """Search for the given string in file and return lines containing that string,
    along with line numbers"""
    line_number = 0
    list_of_results = []
    # Open the file in read only mode
    with open(file_name, 'r') as read_obj:
        # Read all lines in the file one by one
        for line in read_obj:
            # For each line, check if line contains the string
            line_number += 1
            if string_to_search in line:
                # If yes, then search one more time only for today event
                if datetime.utcnow().strftime('%Y-%m-%d') in line:
                    # If yes, then add the line in the list
                    list_of_results.append(line.rstrip())
    # Return list of lines where string is found
    return list_of_results

def check_ping(target_ip):
    response = os.system("ping -c 1 " + target_ip)
    # and then check the response...
    if response == 0:
        pingstatus = "IP:{0} is pingable\n".format(target_ip)
    else:
        pingstatus = "IP:{0} is unpingable\n".format(target_ip)
    utils.log.info(50*"#")
    return pingstatus

#Function to compare 2 list then return difference
def setdiff_sorted(array1,array2,assume_unique=False):
    ans = np.setdiff1d(array1,array2,assume_unique).tolist()
    if assume_unique:
        return sorted(ans)
    return ans

#Function to run cmd on all CVMs
def run_cmd_on_all_cvms(svmips, cmd):
  cmd_map = dict([(x, cmd) for x in svmips])
  status = genesis_utils.run_command_on_svms(cmd_map)
  return status

#Function to get the longest tuple value from dict
def GetMaxValue(dict):        
    maks=max(dict, key=lambda k: len(dict[k][1]))
    return maks,dict[maks]

def print_first_last_line(filtered_list):
    if filtered_list == []:
        utils.log.info("There was no connection error today!")
    else:
        utils.log.info("This is the first time when the connection error occurred")
        utils.log.info(50*"#")
        utils.log.info(filtered_list[0])
        utils.log.info("This is the last time when the connection error occurred")
        utils.log.info(50*"#")
        utils.log.info(filtered_list[-1])
        utils.log.info(50*"#")
    utils.log.info("Current time: {0}".format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
    utils.log.info(50*"#")

#----------------- ONCALL-10927 XIG unreachable due to enabled envoy -----------------#
def oncall_10927():
    utils.log.info(50*"#")
    utils.log.info("Checking ONCALL-10927: Envoy is enabled rather than httpd")
    utils.log.info(50*"#")
    utils.log.info("Checking process on port 9440 ")
    svmips = genesis_utils.get_svm_ips()
    #run cmd from all cvms with the func, dict returns for each cvm.
    allssh_cmd_list = ["sudo netstat -ntpa | grep ':::9440' | grep -i httpd"]

    for i in allssh_cmd_list:
        utils.log.info("Running command: {}".format(i))
        out = run_cmd_on_all_cvms(svmips,i)
        for k,v in out.items():
            filtered_list = v[1].strip().split("\n")
            matches = re.findall(r"\bhttpd\b",filtered_list[0])
            if matches:
                out = "All look good!"
            else: 
                out = "Something went wrong"
            utils.log.info("CVM: {}".format(k))
            utils.log.info(50*"#")
            utils.log.info("{}".format(out))
        utils.log.info("\n")
#----------------- ONCALL-10927 XIG unreachable due to enabled envoy -----------------#


# ---------------------- MAIN  ---------------------#

if __name__ == "__main__":

#Section for collecting all VPN GWs detail from zk
    sub_url = "vpn_gateways/list"
    method = "POST"
    err, resp = invoke_local_rest_api(sub_url, method)
    if not (199 < resp.status_code < 300):
        utils.log.info("Local API request seems failed with {}".format(err))
    respdict = json.loads(resp.text)

    #Checking for direct connect customer, skipping VPN related check
    if len(respdict['entities']) == 0:
        utils.log.info("It seems like direct connect configuration...") 

    #Else for VPN customer
    else:   
    #Section for calling each function to display detail
        get_vpn_detail(err,respdict)
        local_vpn_ip = get_local_vpn_pub_ip(err,respdict)
        remote_vpn_ip = get_remote_vpn_pub_ip(err,respdict)
        
    #Section for running VPN commands against local VPNs to check VPN/BGP status
        for i in local_vpn_ip:
            utils.log.info(50*"#")
            utils.log.info("Running command on local VPN IP: {0} ....\n".format(i))        
            cmd_ike = "ssh vyos@{0} /opt/vyatta/bin/./vyatta-op-cmd-wrapper show vpn ike sa  2>/dev/null".format(i)
            cmd_ipsec = "ssh vyos@{0} /opt/vyatta/bin/./vyatta-op-cmd-wrapper show vpn ipsec sa  2>/dev/null".format(i)
            cmd_bgp = "ssh vyos@{0} /opt/vyatta/bin/./vyatta-op-cmd-wrapper show ip bgp summary  2>/dev/null".format(i)
            cmd_tcp_dump = "ssh vyos@{0} sudo timeout 50s tcpdump -nni eth0 'udp and \(port 500 or 4500\)' > /home/nutanix/dump.txt  2>/dev/null".format(i)
            cmd_filter_tcpdump = "cat /home/nutanix/dump.txt | awk '{print$3}' | sort -ur | grep -v 100.64.1 |cut -d. -f1,2,3,4 ; rm /home/nutanix/dump.txt "

            for j in (cmd_ike,cmd_ipsec,cmd_bgp):
                output = utils.run_cmd(j)
                utils.log.info(output[0])

        #Section for checking https://jira.nutanix.com/browse/NET-3479
        # ----------------------   NET-3479   ---------------------#
            utils.log.info("Checking NET-3479 bug: BGP not advertising")
            utils.log.info(50*"#")
            utils.log.info("Checking bgp advertised-routes ... ")
            #Run cmd to get bgp peer ip
            cmd_get_bgp_peer = "ssh vyos@{0} /opt/vyatta/bin/./vyatta-op-cmd-wrapper sh ip bgp summary  2>/dev/null | grep Neighbor -A1| grep -v Neighbor| cut -d' ' -f1".format(i)
            bgp_peer = utils.run_cmd(cmd_get_bgp_peer)
            #filter str from tuple return
            bgp_peer_ip = bgp_peer[0].strip()
            if not bgp_peer_ip :
                utils.log.info("BGP peer not found..") 
                cmd_static_route = "atlas_cli route_table.get Production | grep Active | grep kStatic"
                if cmd_static_route:
                    utils.log.info("It seems the peer is running with a static route")
                    utils.log.info("Skipping this check")
            else:
                #run cmd to get no of advertised route to peer
                cmd_bgp_adv = "ssh vyos@{0} /opt/vyatta/bin/./vyatta-op-cmd-wrapper sh ip bgp neighbors {1} advertised-routes  2>/dev/null | grep 'prefixes'| cut -d' ' -f5".format(i,bgp_peer_ip)
                bgp_adv = utils.run_cmd(cmd_bgp_adv)
                utils.log.info(50*"#")
                #filter str from tuple return
                bgp_adv_sess = bgp_adv[0].strip()
                if not bgp_adv_sess:
                    #Check to see if VPN tunnel is down
                    ike_stat = utils.run_cmd(cmd_ike)[0].strip()
                    ipsec_stat = utils.run_cmd(cmd_ipsec)[0].strip()
                    #search for "down" string
                    ike_down = re.findall((r'\bdown\b'),ike_stat)
                    ipsec_down = re.findall((r'\bdown\b'),ipsec_stat)
                    if ike_down or ipsec_down:
                        utils.log.info("Seems like VPN tunnel is down")
                    else:
                        utils.log.info("Oops seems like you are hitting the bug(NET-3479)")  
                else:   
                    utils.log.info("Total number of prefixes: {0} being advertised to {1}".format(bgp_adv_sess,bgp_peer_ip))
                    utils.log.info("All look good!!\n")  
        # ----------------------   NET-3479   ---------------------#  

    #Section for running tcpdump command against local VPNs to check incoming IPsec origin IP.
            utils.log.info("Running... tcpdump -nni eth0 'udp and (port 500 or 4500)'")
            tcp_dump = utils.run_cmd(cmd_tcp_dump)
            filter_dump = utils.run_cmd(cmd_filter_tcpdump)
            #init list for vpn origin ip address
            filtered_ip=[]
            ip_pattern = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
            filtered_ip.append(ip_pattern.findall(filter_dump[0].rstrip()))
            #check for no incoming traffic
            if len(filtered_ip[0])== 0:
                utils.log.info("no incoming vpn traffic detected with tcpdump")
            else:
                utils.log.info("##### Here is the tcpdump analysis summary #######")
                utils.log.info(50*"#")
                utils.log.info("Receiving IPsec traffic from Peer IP: {0}".format(filtered_ip[0]))

        #Section for comparing collected origin ip from tcpdump and remote VPN GW public IP.
                check =  all(item in remote_vpn_ip for item in filtered_ip[0])
                if check:
                    utils.log.info("All look good!!")
                else:
                    list_difference = [item for item in filtered_ip[0] if item not in remote_vpn_ip]
                    utils.log.info("## VPN traffic is coming from odd ip {0}".format(list_difference))

#Section for checking OLB IP address and port 1024 to see if L4/L7 layer works   
    #OLB port level check
    olb_address = get_xi_olb_ip()
    #Check curl and wget 
    cmd_curl = "curl -kv  --connect-timeout 5 https://{0}:1024".format(olb_address)
    cmd_wget = "wget --no-check-certificate https://{0}:1024 --connect-timeout=5 -t 1".format(olb_address)

    for i in (cmd_curl,cmd_wget):
        utils.log.info("Running {0}".format(i))
        output = utils.run_cmd(i)
        utils.log.info(output[1])
        utils.log.info(50*"#")

#Section for checking OLB IP address with ping 
    utils.log.info(check_ping(olb_address))

#Section for checking aplos_engine log for the failure timeframe
    #get cvmips from genesis util
    svmips = genesis_utils.get_svm_ips()
    #run cmd from all cvms with the func, dict returns for each cvm.
    out = run_cmd_on_all_cvms(svmips,"egrep 'Failed to create socket for' /home/nutanix/data/logs/aplos_engine.out")
    #select the max tuple among cvm output dict - select master aplos log
    max_pair = GetMaxValue(out)
    #get the log string from the tuple and make a list
    filtered_list = max_pair[1][1].strip().split("\n")
    #get dateformat for today to filter only today's log
    today_str = datetime.today().strftime('%Y-%m-%d')
    #filter log list to contain only today's logs
    today_log = filter(lambda k: today_str in k, filtered_list)
    #print the first and the last line of the log
    print_first_last_line(today_log)

#Section for checking https://jira.nutanix.com/browse/ENG-355137
# ---------------------- ENG-355137  ---------------------#
#Get PE IP list from xat table
    utils.log.info("Checking ENG-355137 bug: XAT is not refreshing")
    utils.log.info(50*"#")
    utils.log.info("Checking Xi cluster ip list from xat ... ")
    #Get AZ id from magneto svc
    cmd_az_id = "links --dump http:0:2070/xat | grep '2070/xat'| tail -1 | cut -d'/' -f5"
    az_id = utils.run_cmd(cmd_az_id)
    str_az_id = az_id[0].strip()
    #find a string for az uuid format
    matches = re.findall(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",str_az_id)

    if not matches :
      utils.log.info("Hmm failed to gather az_uuid, seems like there is no xat config")

    else:
        filtered_azid = matches[0].strip()
        utils.log.info("az uuid:{0}".format(filtered_azid))
        #Filter xat table with az id
        cmd_xat_peip="links --dump http://0:2070/xat/{0} | grep -i untranslated_ip | cut -d':' -f2 | sort -u".format(filtered_azid)
        #get rough ip list from shell cmd
        xat_peip = utils.run_cmd(cmd_xat_peip)
        xat_list =[]
        #find ip pattern then add to the list
        #regex for ip pattern
        ip_pattern = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
        xat_list.append(ip_pattern.findall(xat_peip[0]))
        utils.log.info(sorted(xat_list[0]))

    #Get PC VIP IP list from genesis library

        utils.log.info("Checking PC VIP list from genesis ... ")
        #Filter xat table with az id
        pcip = genesis_utils.get_cluster_external_ip()
        pc_vip_list =[]
        #find ip pattern then add to the list
        pc_vip_list.append(pcip)
        utils.log.info(sorted(pc_vip_list))

    #Get PE IP list from ncli cmd
        utils.log.info("Checking PE cluster ip list from ncli ... ")
        cmd_ncli_peip="source /etc/profile;ncli multicluster get-cluster-state"
        ncli_peip = utils.run_cmd(cmd_ncli_peip)
        ncli_list =[]
        #find ip pattern then add to the list
        ncli_list.append(ip_pattern.findall(ncli_peip[0]))
        utils.log.info(sorted(ncli_list[0]))

    #Compare 2 lists collected then print the difference
        diff = setdiff_sorted(xat_list[0],ncli_list[0])
        check =  all(item in pc_vip_list for item in diff)
        if check:
            utils.log.info("All look good!!")
        else:
            list_difference = [item for item in diff if item not in pc_vip_list]
            utils.log.info("## Stale IP address(es){0} found in XAT table".format(list_difference))
    utils.log.info(50*"#")
# ---------------------- ENG-355137  ---------------------#

#Section for checking https://jira.nutanix.com/browse/NET-3204
# ----------------------   NET-3204   ---------------------#
    utils.log.info("Checking NET-3204 bug: route policy removed mistakenly")
    utils.log.info(50*"#")
    utils.log.info("Checking running route policy... ")
    #get routing policy from api call
    sub_url = "routing_policies/list"
    method = "POST"
    err, resp = invoke_local_rest_api(sub_url, method)
    if not (199 < resp.status_code < 300):
        utils.log.info("Local API request seems failed with {}".format(err))
    respdict = json.loads(resp.text)

    #check no of route policy
    if len(respdict['entities']) == 0:
        utils.log.info("Oops something went wrong!! couldn't find any routing policy")
    else: 
        run_policy = []
        for i in respdict['entities']:
            #Consider only "Production" vpc
            if i['status']['resources']['vpc_reference']['name'] == 'Production':
                run_policy.append(i['status']['resources']['priority'])
        utils.log.info("There are {} routing policy found in Production VPC".format(len(run_policy)))
        utils.log.info("These route policies are running {0}.".format(sorted(run_policy)))

        mantadory_policy =  [1,10,11,12,900,901]
        diff = setdiff_sorted(mantadory_policy,sorted(run_policy))
        if len(diff) == 0:
            utils.log.info("All look good!!")
        else:
            utils.log.info("## Default policy {0} missing".format(diff))

# ----------------------   NET-3204   ---------------------#
    oncall_10927()