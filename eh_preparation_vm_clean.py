#########################################################################
# taeho.choi@nutanix.com
# clean-up stale Preparation VMs created older than N days ago
# 7 Oct 2021                     
# https://jira.nutanix.com/browse/LSEA-184
# https://jira.nutanix.com/browse/ONCALL-11412
#########################################################################

import os 
import sys
import json
import pipes
import re
import argparse
import common_utils as utils
from datetime import datetime, timedelta
from util.base.command import timed_command
from util.net.ssh_client import SSHClient

if os.path.isdir("/usr/local/nutanix"):
    sys.path.insert(0, "/usr/local/nutanix")
    sys.path.insert(0, "/usr/local/nutanix/bin")

import env
from gevent import monkey
monkey.patch_all()
from aplos.client.intentengine.lib.common import invoke_local_rest_api

class Constants:
  VM_NAME_TO_FILTER = 'Preparation -'
  ACLI_CMD = "source /etc/profile;acli vm.list| grep {} ".format(VM_NAME_TO_FILTER)
  LOCALHOST='127.0.0.1' #due to bug on utils.run_cmd method had to use local ssh module 
  REGEX_UUID = re.compile(r'[\w]{8}-[\w]{4}-[\w]{4}-[\w]{4}-[\w]{12}')
  REGEX_UTC_TIME = re.compile(r'd{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2]\d|3[0-1])T(?:[0-1]\d|2[0-3]):[0-5]\d:[0-5]\dZ')
  APLOS_BODY = {"uuid": str}
  APLOS_VM_SUB_URL= 'vms/'

class Helpers:
    @staticmethod
    def nuclei(cmd, options="", timeout=300):
        """Run nuclei with options passed.
        Args:
            cmd(str): Command to run with nuclei.
            options(str): options passed to nuclei.
            timeout(int): Timeout used to run the command.
        Returns:
            str: The output of the nuclei command.
        Raises:
            Exception if nuclei command exit code > 0
        """
        # `timed_command` creates children to run the process
        # without providing an stdin. This results in child processes inheriting
        # the parent process's stdin. nuclei binary has an issue where it expects
        # username/password to be passed though arguments if input is a character
        # device. Redirecting input to /dev/null to overcome this.
        nuclei_cmd = ("/home/nutanix/bin/nuclei --output_format json %s %s < /dev/null" %
                        (options, cmd))
        final_cmd = "bash -c %s" % (pipes.quote(nuclei_cmd))
        ret, out, err = timed_command(final_cmd, timeout_secs=timeout)
        utils.log.debug("cmd=%s, ret=%s, out=%s, err=%s" % (final_cmd, ret, out, err))
        if ret != 0:
            raise Exception(
            "Failed to run command %s using options %s ret=%d, out=%s, err=%s"
            % (cmd, options, ret, out, err)
            )
        return json.loads(out)

    @staticmethod
    def run_cmd_master_node(master_ip,cmd):
        '''
        To run command on the master cvm, need arguments for master and cmd to run
        Will return standard output as a string 
        '''
        hostname = master_ip
        user = "nutanix"
        timeout_secs = 20
        client = SSHClient(hostname,user)
        rv, stdout, stderr = client.execute(cmd, timeout_secs=timeout_secs)

        if not stdout:
            utils.log.info("Command: {0} doesn't have any output".format(cmd))
            sys.exit(0)
        elif rv not in [0,256]:
            utils.log.error("Command: {0} failed for master node: {1} with output: {2} and error: {3}".format(cmd, master_ip, stdout, stderr))
            sys.exit(1)
        return stdout

    @staticmethod
    def is_older_than_ndays(datetime_obj,n):
        '''
        get datetime_obj(datetime obj) and n(int), check whether datetime_obj is older than n days ago from current date.
        if it is older than n days return True or False
        '''
        return datetime.now() - datetime_obj > timedelta(days=n)

    @staticmethod
    def remove_vm_aplos(uuid):
        '''
        get uuid(str) then delete the VM(uuid) by invoking aplos client
        return err(str) and response json format(dict)
        '''
        sub_url = Constants.APLOS_VM_SUB_URL + uuid
        err, resp = invoke_local_rest_api(sub_url,"DELETE",Constants.APLOS_BODY)
        return err,json.loads(resp.text)

def main():
    parser = argparse.ArgumentParser(description='Deleting staled "Preparation -" VM for older than n days(default: 4) from now')
    parser.add_argument('-d','--days',help='specify day to keep',type=int,default=4,required=False,dest='days')
    args =parser.parse_args()

    try:
        #using run_cmd_master_node method due to bug on run_cmd()
        vmlist = Helpers.run_cmd_master_node(Constants.LOCALHOST,Constants.ACLI_CMD)
        utils.log.info("This is the VM list with {} \n{}".format(Constants.VM_NAME_TO_FILTER,vmlist))
        uuid_lst = Constants.REGEX_UUID.findall(vmlist)

        if uuid_lst:
            utils.log.info("{} VM(s) found".format(len(uuid_lst)))
            utils.log.info("Checking creation time ...")
            uuid_to_delete=[]
            for i in uuid_lst:  
                nuclei_cmd = ' vm.get {}'.format(i)
                stdout = Helpers.nuclei(nuclei_cmd)
                utc_time_str =  stdout['data']['metadata'].get('creation_time')
                date_time_obj = datetime.strptime(utc_time_str,"%Y-%m-%dT%H:%M:%SZ")
                utils.log.info("Creation time for VM uuid :{} is {}".format(i,date_time_obj.strftime('%Y-%m-%d %H:%M:%S')))
                #Find uuid older than N days
                if Helpers.is_older_than_ndays(date_time_obj,args.days):
                    uuid_to_delete.append(i)
            utils.log.info("Found {} VMs older than {} days".format(len(uuid_to_delete),args.days))
            utils.log.info("Current date is {}".format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))

            #Delete VMs in the uuid_to_delete list
            for i in uuid_to_delete:
                utils.log.info("Deleting VM UUID:{}".format(i))
                err,output = Helpers.remove_vm_aplos(i)
                utils.log.info(output) if not err else utils.log.error(err)

        else:
            utils.log.info("No VM UUID found to check")
            sys.exit(0)

    except Exception as err:
        utils.log.error("No VM list found {}".format(err))

if __name__ == "__main__":
    sys.exit(main())