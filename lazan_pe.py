import os 
import sys
import logging
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

cwd = os.getcwd()
if os.path.isdir(cwd):
    sys.path.insert(2, cwd)
import common_utils as utils

def run_cmd_on_all_cvms(svmips, cmd):
  cmd_map = dict([(x, cmd) for x in svmips])
  status = genesis_utils.run_command_on_svms(cmd_map)
  return status

def main():
    #Collect svmips list from genesis util
    svmips = genesis_utils.get_svm_ips()

    #----------------- Section for check allssh cmd -----------------#
    #run cmd from all cvms with the func, dict returns for each cvm.
    allssh_cmd_list = ['cat ~/config/upgrade.history | tail -2',
                'grep "needs to be processed before removal" /home/nutanix/data/logs/genesis.out | head -2',
                'grep remove /home/nutanix/data/logs/cluster_sync.out | tail -2',
                'grep "Committing expand cluster params" /home/nutanix/data/logs/genesis.out*',
                'grep "Added node to cluster" /home/nutanix/data/logs/genesis.out',
                'grep "ExtentGroupsToMigrateFromDisk" /home/nutanix/data/logs/curator.INFO',
                'grep "NumOplogEpisodesInToRemoveDisk" /home/nutanix/data/logs/curator.INFO',
                'grep "NearSyncOplogEpisodesInToRemoveDisk" /home/nutanix/data/logs/curator.INFO',
                'grep "Failed to get software version from node" /home/nutanix/data/logs/genesis.out*',
                '/usr/local/nutanix/cluster/bin/list_disks | grep "dev/sd" | wc -l',
                '/usr/bin/lsscsi | grep "dev/sd" | wc -l',
                'grep hba_address /etc/nutanix/hardware_config.json',
                #HP node only for HBA cable location 'sudo cat /sys/block/sd*/device/path_info', grep hba_address /etc/nutanix/hardware_config.json  - https://nutanix.slack.com/archives/C01UUH2A85C/p1619544739047000
                'find /home/nutanix/data/logs/ -name *.FATAL -mmin -600']

    for i in allssh_cmd_list:
        utils.log.info("Running command: {}".format(i))
        out = run_cmd_on_all_cvms(svmips,i)
        for k,v in out.iteritems():
            filtered_list = v[1].strip().split("\n")
            utils.log.info("CVM: {}".format(k))
            utils.log.info(50*"#")
            utils.log.info("\n".join(filtered_list))
        utils.log.info("\n")
    #----------------- Section for check allssh cmd -----------------#

    #----------------- Section ncli/ssh cmd -----------------#
    cmd_list = ['source /etc/profile;ncli host ls',
                'source /etc/profile;acli host.list',
                'source /etc/profile;ncli cluster info | grep -i "full version"',
                'source /etc/profile;ncli host get-rm-status',
                'source /etc/profile;ncli disk get-rm-status',
                'source /etc/profile;ncli pd ls-repl-status protection-domain-type=entity-centric',
                'source /etc/profile;zeus_config_printer | grep -B3 -A1 kToBeRemoved',
                'source /etc/profile;zeus_config_printer | grep -B8 -A3 to_remove',
                'source /etc/profile;ncli cluster get-domain-fault-tolerance-status type=node | grep Tolerance',
                'source /etc/profile;svmips',
                'source /etc/profile;progress_monitor_cli --fetchall',
                'source /etc/profile;svmips | wc -w',
                'source /etc/profile;nodetool -h 0 ring | grep Normal | wc -l',
                'source /etc/profile;nodetool -h 0 ring | grep Up | wc -l']

    for i in cmd_list:
        utils.log.info("Running command: {}".format(i))
        utils.log.info(50*"#")
        out = utils.run_cmd(i)
        utils.log.info(out[0].rstrip())
    #----------------- Section ncli/ssh cmd -----------------#   

if __name__ == "__main__":
    main()