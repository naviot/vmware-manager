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

    def nic_info(self, nic_type='Vmxnet3', mac_address=None):
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


class Spawner(Vcenter_base, Vcenter_obj_tpl):
    pass

