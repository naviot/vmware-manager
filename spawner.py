#!/usr/bin/python
import sys
import atexit

from pyVim import connect
from pyVmomi import vim
from pyVmomi import vmodl


class Vcenter_base(object):
    def __init__(self, user_data):
        self.vc_ip = user_data['vcenter_ip']
        self.vc_user = user_data['vcenter_user']
        self.vc_pass = user_data['vcenter_password']

    def connect_to_vcenter(self):
        self.service_instance = connect.SmartConnect(host=self.vc_ip,
                                                     user=self.vc_user,
                                                     pwd=self.vc_pass,
                                                     port=443)
        self.content = self.service_instance.RetrieveContent()
        self.network_folder = self.content.rootFolder.childEntity[0].networkFolder
        atexit.register(connect.Disconnect, self.service_instance)

    def wait_for_tasks(self, service_instance, tasks):
        """Given the service instance si and tasks, it returns after all the
       tasks are complete
       """
        property_collector = service_instance.content.propertyCollector
        task_list = [str(task) for task in tasks]
        # Create filter
        obj_specs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task)
                     for task in tasks]
        property_spec = vmodl.query.PropertyCollector.PropertySpec(type=vim.Task,
                                                                   pathSet=[],
                                                                   all=True)
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.objectSet = obj_specs
        filter_spec.propSet = [property_spec]
        pcfilter = property_collector.CreateFilter(filter_spec, True)
        try:
            version, state = None, None
            # Loop looking for updates till the state moves to a completed state.
            while len(task_list):
                update = property_collector.WaitForUpdates(version)
                for filter_set in update.filterSet:
                    for obj_set in filter_set.objectSet:
                        task = obj_set.obj
                        for change in obj_set.changeSet:
                            if change.name == 'info':
                                state = change.val.state
                            elif change.name == 'info.state':
                                state = change.val
                            else:
                                continue

                            if not str(task) in task_list:
                                continue

                            if state == vim.TaskInfo.State.success:
                                # Remove task from taskList
                                task_list.remove(str(task))
                            elif state == vim.TaskInfo.State.error:
                                raise task.info.error
                # Move to next version
                version = update.version
        finally:
            if pcfilter:
                pcfilter.Destroy()

    def get_obj(self, vimtype, name):
        """
        Get the vsphere object associated with a given text name
        """
        obj = None
        container = self.content.viewManager.CreateContainerView(self.content.rootFolder, vimtype, True)
        for c in container.view:
            if c.name == name:
                obj = c
                break
        return obj


class Vcenter_obj_tpl(object):
    def controller_info(self):
        scsi_spec = vim.vm.device.VirtualDeviceSpec()
        scsi_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        scsi_spec.device = vim.vm.device.VirtualLsiLogicController()
        scsi_spec.device.deviceInfo = vim.Description()
        scsi_spec.device.slotInfo = vim.vm.device.VirtualDevice.PciBusSlotInfo()
        scsi_spec.device.slotInfo.pciSlotNumber = 16
        scsi_spec.device.controllerKey = 100
        scsi_spec.device.unitNumber = 3
        scsi_spec.device.busNumber = 0
        scsi_spec.device.hotAddRemove = True
        scsi_spec.device.sharedBus = 'noSharing'
        scsi_spec.device.scsiCtlrUnitNumber = 7
        return scsi_spec

    def disk_info(self, disk_size, controller_info, unit_number=0):
        new_disk_kb = int(disk_size) * 1024 * 1024
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.fileOperation = "create"
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add

        disk_spec.device = vim.vm.device.VirtualDisk()
        disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        disk_spec.device.backing.diskMode = 'persistent'
        disk_spec.device.unitNumber = unit_number
        disk_spec.device.capacityInKB = new_disk_kb
        disk_spec.device.controllerKey = controller_info.device.key
        return disk_spec

    def nic_info(self, nic_type='Vmxnet3', mac_address=None, label=None):
        nic_spec = vim.vm.device.VirtualDeviceSpec()
        nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        if nic_type == 'Vmxnet3':
            nic_spec.device = vim.vm.device.VirtualVmxnet3()
        elif nic_type == 'E1000':
            nic_spec.device = vim.vm.device.VirtualE1000()
        if mac_address:
            nic_spec.device.macAddress = mac_address
        nic_spec.device.deviceInfo = vim.Description()
        nic_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
        nic_spec.device.connectable.startConnected = True
        nic_spec.device.connectable.allowGuestControl = True
        nic_spec.device.connectable.connected = True
        nic_spec.device.wakeOnLanEnabled = True
        nic_spec.device.addressType = 'assigned'
        if label:
            nic_spec.device.deviceInfo.label = label
        port = vim.dvs.PortConnection()
        # port.switchUuid = '54 01 36 50 13 35 cf f0-3d ad c9 74 36 95 2f 7e'
        # port.portgroupKey = 'dvportgroup-43'
        nic_spec.device.backing = vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo()
        nic_spec.device.backing.port = port
        return nic_spec

    def vmx_file_info(self, storage_name, vm_name):
        datastore_path = '[' + storage_name + '] ' + vm_name
        vmx_file = vim.vm.FileInfo(logDirectory=None,
                                   snapshotDirectory=None,
                                   suspendDirectory=None,
                                   vmPathName=datastore_path)
        return vmx_file

    def dvs_info(self, dvs_name):
        pvlan_configs = []
        dvs_create_spec = vim.DistributedVirtualSwitch.CreateSpec()
        dvs_config_spec = vim.dvs.VmwareDistributedVirtualSwitch.ConfigSpec()
        for pvlan_idx in range(100, 2001, 2):
            # promiscuous  pvlan config
            pvlan_map_entry = vim.dvs.VmwareDistributedVirtualSwitch.PvlanMapEntry()
            pvlan_config_spec = vim.dvs.VmwareDistributedVirtualSwitch.PvlanConfigSpec()
            pvlan_map_entry.primaryVlanId = pvlan_idx
            pvlan_map_entry.secondaryVlanId = pvlan_idx
            pvlan_map_entry.pvlanType = "promiscuous"
            pvlan_config_spec.pvlanEntry = pvlan_map_entry
            pvlan_config_spec.operation = vim.ConfigSpecOperation.add
            # isolated pvlan config
            pvlan_map_entry2 = vim.dvs.VmwareDistributedVirtualSwitch.PvlanMapEntry()
            pvlan_config_spec2 = vim.dvs.VmwareDistributedVirtualSwitch.PvlanConfigSpec()
            pvlan_map_entry2.primaryVlanId = pvlan_idx
            pvlan_map_entry2.secondaryVlanId = pvlan_idx + 1
            pvlan_map_entry2.pvlanType = "isolated"
            pvlan_config_spec2.pvlanEntry = pvlan_map_entry2
            pvlan_config_spec2.operation = vim.ConfigSpecOperation.add
            pvlan_configs.append(pvlan_config_spec)
            pvlan_configs.append(pvlan_config_spec2)
        dvs_config_spec.pvlanConfigSpec = pvlan_configs

        dvs_config_spec.name = dvs_name
        dvs_create_spec.configSpec = dvs_config_spec
        return dvs_create_spec

    def dvs_host_info(self, host, uplink = None):
        dvs_host_configs = list()
        uplink_port_names = 'dvUplink1'
        dvs_config_spec = vim.DistributedVirtualSwitch.ConfigSpec()
        dvs_config_spec.uplinkPortPolicy = vim.DistributedVirtualSwitch.NameArrayUplinkPortPolicy()
        dvs_config_spec.uplinkPortPolicy.uplinkPortName = uplink_port_names
        dvs_config_spec.maxPorts = 60000
        dvs_host_config = vim.dvs.HostMember.ConfigSpec()
        dvs_host_config.operation = vim.ConfigSpecOperation.add
        dvs_host_config.host = host
        if uplink:
            pnic_specs = list()
            pnic_spec = vim.dvs.HostMember.PnicSpec()
            pnic_spec.pnicDevice = uplink
            pnic_specs.append(pnic_spec)
            dvs_host_config.backing = vim.dvs.HostMember.PnicBacking()
            dvs_host_config.backing.pnicSpec = pnic_specs
        dvs_host_configs.append(dvs_host_config)
        dvs_config_spec.host = dvs_host_configs
        return dvs_config_spec



class Vm(Vcenter_base, Vcenter_obj_tpl):
    def __init__(self, user_data):
        super(Vm, self).__init__(user_data)
        self._unit_number = None
        self._nic_unit_number = None
        self.vm_devices = list()
        self.scsi_spec = self.controller_info()
        self.vm_devices.append(self.scsi_spec)

    def add_disk(self, disk_size):
        if self._unit_number >= 0:
            self._unit_number += 1
        else:
            self._unit_number = 0

        disk_spec = self.disk_info(disk_size, self.scsi_spec, self._unit_number)
        self.vm_devices.append(disk_spec)

    def add_nic(self, nic_type='Vmxnet3', mac_address=None):
        if self._nic_unit_number >= 0:
            self._nic_unit_number += 1
        else:
            self._nic_unit_number = 0

        label = 'nic' + str(self._nic_unit_number)
        nic_spec = self.nic_info(nic_type, mac_address, label)
        self.vm_devices.append(nic_spec)

    def create(self, name, cpu, memory, storage_name, cluster=None, host=None):
        if host:
            host_obj = self.get_obj([vim.HostSystem], host)
            vm_folder = host_obj.parent.parent.parent.vmFolder
            resource_pool = host_obj.parent.resourcePool
        elif cluster:
            host_obj = None
            cluster_obj = self.get_obj([vim.ClusterComputeResource], cluster)
            vm_folder = cluster_obj.parent.parent.vmFolder
            resource_pool = cluster_obj.resourcePool
        else:
            print "Need to specify Cluster or Host name where you want to create vm."
            sys.exit(1)

        vmx_file = self.vmx_file_info(storage_name, name)

        self.config = vim.vm.ConfigSpec(
            name=name,
            memoryMB=memory,
            numCPUs=cpu,
            guestId="ubuntu64Guest",
            files=vmx_file,
            deviceChange=self.vm_devices,
        )

        print "Creating VM {}...".format(name)

        task = vm_folder.CreateVM_Task(config=self.config, pool=resource_pool, host=host_obj)
        self.wait_for_tasks(self.service_instance, [task])


class Dvs(Vcenter_base, Vcenter_obj_tpl):
    def create_dvs(self, dvs_name):
        dvs_spec = self.dvs_info(dvs_name)

        print "Creating DVS: ", dvs_name

        task = self.network_folder.CreateDVS_Task(dvs_spec)
        self.wait_for_tasks(self.service_instance, [task])

    def add_hosts(self, hosts_list, dvs_name, attach_uplink):
        dvs = self.get_obj([vim.DistributedVirtualSwitch], dvs_name)

        for h in hosts_list:
            host = h['host']
            uplink = h['uplink']
            if not attach_uplink:
                uplink = None
            host_obj = self.get_obj([vim.HostSystem], host)
            dvs_host_spec = self.dvs_host_info(host_obj, uplink)
            dvs_host_spec.configVersion = dvs.config.configVersion
            print "Adding {} to DVS: {}".format(host, dvs_name)
            task = dvs.ReconfigureDvs_Task(dvs_host_spec)
            self.wait_for_tasks(self.service_instance, [task])



if __name__ == '__main__':
    user_data = {'vcenter_ip': '172.16.0.145', 'vcenter_user': 'root', 'vcenter_password': 'vmware'}
    hosts = [{'host': '172.16.0.250', 'uplink': 'vmnic1'}, {'host': '172.16.0.252', 'uplink': 'vmnic1'}, {'host': '172.16.0.253', 'uplink': 'vmnic1'}]

    # spawn = Spawner(user_data)
    # spawn.connect_to_vcenter()
    # spawn.create_vm(name='TestVM5', flavor=vm_flavor, cluster='Cluster1', storage_name='nfs')

    # vm = Vm(user_data)
    # vm.connect_to_vcenter()
    # vm.add_disk(1)
    # vm.add_nic()
    # vm.create(name='TestVM7', cpu=1, memory=128, storage_name='nfs', cluster='Cluster1')

    # dvs = Dvs(user_data)
    # dvs.connect_to_vcenter()
    # dvs.create_dvs('Contrail')
    # dvs.add_hosts(hosts_list=hosts, dvs_name='Contrail', attach_uplink=False)
