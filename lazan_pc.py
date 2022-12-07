#########################################################################
# Synopsis
# Taeho Choi taeho.choi@nutanix.com                         
# This script collect log from pc cluster regarding lazan addnode/remove node task
# Also parse curator log for node/disk removal status       Mar/19/2021
# Added to collect PE node detail from lazan master         Mar/22/2021
# Replaced print with logging moduel for logz.io            June/8/2021
#########################################################################

import os 
import sys
from datetime import datetime

if os.path.isdir("/usr/local/nutanix"):
    sys.path.insert(0, "/usr/local/nutanix")
    sys.path.insert(0, "/usr/local/nutanix/bin")
    import env
    from gevent import monkey
    monkey.patch_all()

    import cluster.genesis_utils as genesis_utils
    import re
    import subprocess

    from util.net.ssh_client import SSHClient
    import json

cwd = os.getcwd()
if os.path.isdir(cwd):
    sys.path.insert(2, cwd)
import common_utils as utils

#Function to print the fist line and last line of log
def print_first_last_line(filtered_list):
    if filtered_list == []:
        utils.log.info("There is no entry")
    else:
        utils.log.info("\nThis is the first entry from the current lazan.out")
        utils.log.info(50*".")
        utils.log.info(filtered_list[0])
        utils.log.info("This is the last entry from the current lazan.out")
        utils.log.info(50*".")
        utils.log.info(filtered_list[-1])
        utils.log.info(50*".")
    utils.log.info("Current time: {0}".format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
    utils.log.info(50*".")

def sh_cluster_ver_ncli():
    """
    Get cluster version from ncli
    """
    get_cmd = ("source /etc/profile; /home/nutanix/prism/cli/ncli"
                " cluster info --json true | grep Version")
    stdout,stderr = utils.run_cmd(get_cmd)
    data = json.loads(stdout.strip())
    utils.log.info("Collecting PC cluster version from ncli..")
    utils.log.info(data['data']["fullVersion"])
    return True

def run_cmd_on_all_cvms(svmips, cmd):
  cmd_map = dict([(x, cmd) for x in svmips])
  status = genesis_utils.run_command_on_svms(cmd_map)
  return status

def check_ENG_340604():
    #https://jira.nutanix.com/browse/ENG-340604
    utils.log.info(50*"#")
    utils.log.info("Checking disable_trm ENG-340604 bug: disable remove node feat on 5.18 ")
    utils.log.info(50*"#")
    utils.log.info("PC cluster ver from ncli ")
    sh_cluster_ver_ncli()
    #Section for checking neutron gflag for the issue
    #get cvmips from genesis util
    svmips = genesis_utils.get_svm_ips()
    #run cmd from all cvms with the func, dict returns for each cvm.
    utils.log.info("gflag status from neuron svc supposed to be false/False")
    out = run_cmd_on_all_cvms(svmips,"grep 'enable_backgroundtask' /home/nutanix/config/neuron.gflags")
    for key,value in out.iteritems():
        utils.log.info("CVM IP:{} gflag:{}".format(key,value[1].strip()))
    return True

def idf_node_detail_check():
    utils.log.info(50*"#")
    utils.log.info("Checking individual node status from idf ")
    utils.log.info(50*"#")
    idf_node_cmd = "links --dump http://0:2027/all_entities?type=node | grep 'type=node'"
    idf_node_detail = utils.run_cmd(idf_node_cmd)
    uuid_list=[]
    uuid_regex = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
    uuid_list.append(uuid_regex.findall(idf_node_detail[0].rstrip()))
    utils.log.info("This is node uuid list in idf: {}\n".format(uuid_list[0]))

    for i in uuid_list[0]:
        idf_single_node_cmd = 'links --dump http://0:2027/entities?id={} |grep "Insights DB" -A30'.format(i)
        idf_single_detail = utils.run_cmd(idf_single_node_cmd)
        utils.log.info("node uuid:{0}\n {1}".format(i,idf_single_detail[0]))

def lazan_task_list():
    utils.log.info(50*"#")
    utils.log.info("Checking to see if there is any active task for Lazan ")
    utils.log.info(50*"#")
    ecli_lazan_cmd1 = "source /etc/profile;ecli task.list component_list=Lazan limit=10000 | egrep -i 'kQueued|kRunning'| wc -l "
    ecli_lazan_cmd2 = "source /etc/profile;ecli task.list component_list=Lazan limit=10000 | egrep -i 'kQueued|kRunning'| head -2 "
    ecli_lazan_cmd3 = "source /etc/profile;ecli task.list component_list=Lazan limit=10000 | egrep -i 'kQueued|kRunning'| tail -2 "
    for i in [ecli_lazan_cmd1,ecli_lazan_cmd2,ecli_lazan_cmd3]:
        out = utils.run_cmd(i)
        utils.log.info("{}".format(out[0]))

def main():
    #Section to get Lazan master node
    cmd_lazan_master = "links -dump http:0:2038 | grep Master"
    lazan_master = utils.run_cmd(cmd_lazan_master)
    ip_pattern = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')
    master_ip = ip_pattern.findall(lazan_master[0])
    utils.log.info("Lazan master ip:{0}".format(master_ip[0]))

    #Section to filter log from Lazan master node
    hostname = master_ip[0]
    user = "nutanix"
    timeout_secs = 20
    client = SSHClient(hostname,user)
    cmd = ("source /etc/profile;cat /home/nutanix/data/logs/lazan.out")
    rv, stdout, stderr = client.execute(cmd, timeout_secs=timeout_secs)
    if rv != 0:
        utils.log.info("Hmm something went wrong with lazan.out")
        return
    else:
        log_list = []
        log_list = stdout.strip().split("\n")

        string_to_check = [ #"raw storage capacity",
                            "Starting Xi PC scheduler",
                            "TRM error kNoResource: There is no resource in Xi",
                            "Client has either been stopped or not yet registered",
                            "(ScheduleVmDRSnapshot) failed with message",
                            "No cluster available with given resource requirements",
                            "but we are trying to set to",
                            "for removal",
                            "Required storage",
                            "TRM task didn't complete in time",   #https://jira.nutanix.com/browse/ENG-348636
                            "Added nodes",
                            "Done processing Lazan request ScheduleVmDRSnapshot locally",
                            "reservation for cluster 00000000-0000-0000-0000-000000000000", #https://jira.nutanix.com/browse/ONCALL-10496, https://nutanix.slack.com/archives/C01JNJW1GBB/p1611766680057100
                            "failed with message: Cluster expansion for tenant",
                            "failed with message: LazanAddNode task",
                            "TRM task didn't complete in time",
                            "No cluster can accommodate the maximum compute requirements in this batch of specs" #unable to cope with the largest VM spec https://portal.nutanix.com/page/documents/details?targetId=Leap-Xi-Leap-Admin-Guide-v5_19:ecd-ecdr-draas-limitations-pc-r.html
                            ]

        for i in string_to_check:
            utils.log.info("\nChecking for string {}..".format(i))
            utils.log.info(50*"#")
            filtered_log = filter(lambda k: i in k, log_list)
            #print the first and the last line from filtered log list
            utils.log.info("\n".join(filtered_log))
            print_first_last_line(filtered_log)

    #Section for collecting node/cluster list detail
    utils.log.info(50*"#")
    utils.log.info("Checking PE node detail from lazan master: ")
    utils.log.info(50*"#")
    cmd_node_res = ("source /etc/profile;links --dump http://{}:2038/reservations | grep 'Node Information' -A100 | awk -vRS='Cluster Information' ' /Node Information/ '".format(master_ip[0]))
    stdout, stderr = utils.run_cmd(cmd_node_res)
    if stderr :
        utils.log.info("Hmm something went wrong with collecting node detail")
        return
    else:
        utils.log.info("This is the node detail on Lazan master:\n\n {}".format(stdout))
    #Section end for collecting node/cluster list detail 

    #Section for checking node detail from idf
    idf_node_detail_check()

    #Section for checking lazan ergon task list
    lazan_task_list()

    #Section for checking known ENG_340604 bug
    check_ENG_340604()

if __name__ == "__main__":
    main()