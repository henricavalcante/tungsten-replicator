require "#{File.dirname(__FILE__)}/snapshot_backup"

begin
  require 'rubygems'
rescue LoadError
end

begin
  require 'aws/ec2'
rescue LoadError
  raise "The aws-sdk Ruby gem or rubygem-aws-sdk package is required for this class"
end

# TODO: Snapshot when there is an lvm
# TODO: Hooks before/after the snapshot/restore to modify the list of 
# directories or paths to backup
class EBSSnapshotBackup < TungstenSnapshotBackup
  def create_snapshot(timestamp)
    snapshots = {}
    mount_point_ids = {}
    path_ids = {}
    ds = TI.datasource(opt(:service))
    
    ec2 = get_ec2()
    instance = get_instance()
    region = get_region()
    
    ds.snapshot_paths.each{
      |path|
      mount_point = get_mount_point(path)
      if mount_point.to_s() == ""
        TU.error("Unable to find a mount point for #{path}")
      end
    }
    unless TU.is_valid?()
      return false
    end
    
    ds.snapshot_paths.each{
      |path|
      mount_point = get_mount_point(path)
      if mount_point_ids.has_key?(mount_point)
        TU.debug("Skipping snapshot of #{path} because #{mount_point} has already been processed")
        path_ids[path] = {
          "mount_point" => mount_point
        }
      else
        TU.debug("Take a snapshot of #{mount_point} for #{path}")
        
        attachment = find_attachment_for_mount_point(mount_point, instance)
        volume = attachment.volume
        TU.debug("The EBS volume ID for #{mount_point} is #{volume.id}")
        
        # Collect the volume type this way because the AWS-SDK uses
        # the Ruby reserved word 'type' instead of something that would 
        # work in Ruby 1.8
        volume_type = TU.cmd_result(". /etc/profile.d/aws-apitools-common.sh; /opt/aws/bin/ec2-describe-volumes #{volume.id} --region #{region.name} --show-empty-fields --hide-tags | grep VOLUME | awk -F' ' '{print $8}'")
        
        # Create and tag the snapshot
        snapshot = volume.create_snapshot("#{timestamp}:#{attachment.device}")
        snapshot.add_tag('ContinuentTungstenBackupMethod', :value => self.class.name())
        snapshot.add_tag('ContinuentTungstenBackupID', :value => timestamp)
        snapshot.add_tag('ContinuentTungstenSnapshotInstanceID', :value => instance.id)
        snapshot.add_tag('ContinuentTungstenSnapshotDeviceMountPoint', :value => mount_point)
        snapshot.add_tag('ContinuentTungstenSnapshotEBSMountPoint', :value => attachment.device)
        snapshot.add_tag('ContinuentTungstenSnapshotEBSVolumeID', :value => volume.id)
        snapshot.add_tag('ContinuentTungstenSnapshotEBSVolumeType', :value => volume_type)
        snapshot.add_tag('ContinuentTungstenSnapshotEBSVolumeIOPS', :value => volume.iops)
        snapshot.add_tag('ContinuentTungstenSnapshotEBSVolumeSize', :value => volume.size)

        snapshots[snapshot.id] = snapshot
        mount_point_ids[mount_point] = {
          "snapshot_id" => snapshot.id,
          "region" => region.name
        }
        path_ids[path] = {
          "snapshot_id" => snapshot.id,
          "region" => region.name,
          "mount_point" => mount_point
        }
        TU.debug("Created snapshot #{snapshot.id} for #{mount_point}")
      end
    }
    
    TU.debug("Waiting for all snapshots to complete or fail")
    wait_for_snapshots(snapshots.values())    
    unless TU.is_valid?()
      return false
    end
    
    path_ids
  end
  
  def restore_snapshot(snapshot_ids)
    rollback_volumes = true
    temporary_snapshots = []
    
    mount_point_snapshot_info = {}
    mount_point_snapshots = {}
    mount_point_original_volumes = {}
    mount_point_original_attachments = {}
    mount_point_replacement_volumes = {}
    mount_point_replacement_attachments = {}
    
    ds = TI.datasource(opt(:service))
    
    ec2 = get_ec2()
    instance = get_instance()
    region = get_region()
    
    # Confirm that all paths that share a mount point on this host
    # are from the same snapshot in the backup
    snapshot_ids.each{
      |path, info|
      if info["snapshot_id"] == nil
        # Skip because this path does not have a snapshot_id
        # This can happen if multiple paths are located on the same volume
        next
      end
      
      mount_point = info["mount_point"]
      unless mount_point_snapshot_info.has_key?(mount_point)
        mount_point_snapshot_info[mount_point] = info
        
        begin
          attachment = find_attachment_for_mount_point(mount_point, instance)
          mount_point_original_attachments[mount_point] = attachment
          mount_point_original_volumes[mount_point] = attachment.volume
        rescue => e
          TU.warning("Unable to find a volume for #{mount_point}")
        end
      else
        # If the snapshot_id for this path is not the same as for other 
        # paths on the same mount_point, then there must be differences between
        # the system configurations
        if info["snapshot_id"] != mount_point_snapshot_info[mount_point]["snapshot_id"]
          TU.error("The mount point for #{path} is not on the same volume as where the backup was taken")
        end
      end
    }
    
    unless TU.is_valid?()
      return false
    end
        
    begin
      # Find the snapshot object for each mount point
      # If the snapshot is in a different region, it will be copied
      # to the current region.
      mount_point_snapshot_info.each{
        |mount_point, info|
        snapshot_region = info["region"]
        original_snapshot_id = info["snapshot_id"]

        if snapshot_region != region.name
          # Copy to the correct region using ec2-copy-snapshot
          begin
            original_snapshot = ec2.regions[snapshot_region].snapshots[original_snapshot_id]
            unless original_snapshot.exists?()
              TU.error("Unable to find snapshot #{original_snapshot_id}@#{snapshot_region}")
              next
            end
            
            TU.debug("Copy #{original_snapshot.id}@#{snapshot_region} to #{region.name}")
            snapshot_id = TU.cmd_result(". /etc/profile.d/aws-apitools-common.sh; /opt/aws/bin/ec2-copy-snapshot --region #{region.name} -r #{snapshot_region} -s #{original_snapshot.id} --description '#{self.class.name} copy of #{original_snapshot.id} from #{snapshot_region}' | awk -F' ' '{print $2}'")        
            snapshot = region.snapshots[snapshot_id]
            temporary_snapshots << snapshot
            TU.debug("Copying #{original_snapshot.id}@#{snapshot_region} to #{snapshot_id}@#{region.name}")
            
            original_snapshot.tags.to_h().each{
              |k,v|
              snapshot.add_tag(k, :value => v)
            }
            
            mount_point_snapshots[mount_point] = snapshot
          rescue => e
            TU.exception(e)
            raise "Unable to copy snapshot #{original_snapshot_id}@#{snapshot_region} to #{region.name}"
          end
        else
          # Find the local snapshot
          snapshot = region.snapshots[original_snapshot_id]
          unless snapshot.exists?()
            TU.error("Unable to find snapshot #{original_snapshot_id}@#{region.name}")
            next
          end
          mount_point_snapshots[mount_point] = snapshot
        end
      }
      
      TU.debug("Waiting for all copied snapshots to complete or fail")
      wait_for_snapshots(temporary_snapshots)
      unless TU.is_valid?()
        return false
      end
      
      # Create an EBS volume for every snapshot
      mount_point_snapshots.each{
        |mount_point, snapshot|
        TU.debug("Create volume from #{snapshot.id} for #{mount_point}")
        
        # Create the new volume
        snapshot_tags = snapshot.tags.to_h()
        
        if snapshot_tags["ContinuentTungstenSnapshotEBSVolumeType"] == "io1"
          if snapshot_tags["ContinuentTungstenSnapshotEBSVolumeIOPS"] == ""
            iops = ""
          else
            iops = snapshot_tags["ContinuentTungstenSnapshotEBSVolumeIOPS"].to_i()
          end

          volume = region.volumes.create(
            :snapshot => snapshot,
            :availability_zone => instance.availability_zone,
            :size => snapshot_tags["ContinuentTungstenSnapshotEBSVolumeSize"].to_i(),
            :volume_type => snapshot_tags["ContinuentTungstenSnapshotEBSVolumeType"],
            :iops => iops
          )
        else
          volume = region.volumes.create(
            :snapshot => snapshot,
            :availability_zone => instance.availability_zone,
            :size => snapshot_tags["ContinuentTungstenSnapshotEBSVolumeSize"].to_i(),
            :volume_type => snapshot_tags["ContinuentTungstenSnapshotEBSVolumeType"]
          )
        end
        volume.add_tag('ContinuentTungstenBackupMethod', :value => self.class.name())
        
        mount_point_replacement_volumes[mount_point] = volume
        TU.debug("Volume #{volume.id} created from #{snapshot.id}")
      }
      
      TU.debug("Waiting for all volumes to be created")
      mount_point_replacement_volumes.each{
        |mount_point, volume|
        sleep 15 until [:available, :error].include?(volume.status)

        case volume.status
        when :available
          TU.debug("The creation of volume #{volume.id} has finished")
        when :error
          TU.error("The creation of volume #{volume.id} failed")
        end
      }
      
      unless TU.is_valid?()
        return false
      end
      
      # Unmount and detach the original volumes
      mount_point_original_attachments.each{
        |mount_point, attachment|
        TU.debug("Unmount volume at #{mount_point}")

        begin
          is_mounted = TU.cmd_result("#{TI.sudo_prefix()}df -h | egrep '^#{mount_point} ' | wc -l").to_i()
        rescue CommandError
          TU.error("Unable to check mount status for #{mount_point}")
          next
        end

        if is_mounted == 1
          begin
            TU.cmd_result("#{TI.sudo_prefix()}/bin/umount #{mount_point}")
          rescue CommandError
            TU.error("There was an error while unmounting #{mount_point}")
            next
          end
        end
      }

      unless TU.is_valid?()
        return false
      end
      
      remove_volume_attachments(mount_point_original_attachments, opt(:delete_original_volumes))
      unless TU.is_valid?()
        return false
      end
      
      # Attach and mount the original volumes
      mount_point_replacement_volumes.each{
        |mount_point, volume|
        matches = mount_point.match("/dev/xvd([a-z]+)")
        if matches && matches.size() > 0
          mount_point = "/dev/sd#{matches[1]}"
        end

        TU.debug("Attach volume #{volume.id} on #{mount_point}")
        mount_point_replacement_attachments[mount_point] = volume.attach_to(instance, mount_point)
      }

      TU.debug("Waiting for all volumes to be attached")
      mount_point_replacement_attachments.each{
        |mount_point, attachment|
        sleep 15 until [:attached, :error].include?(attachment.status)

        case attachment.status
        when :attached
          TU.debug("The attachment of volume #{attachment.volume.id} has finished")
        when :error
          TU.error("The attachment of volume #{attachment.volume.id} failed")
        end
      }
      
      unless TU.is_valid?()
        return false
      end
      
      rollback_volumes = false
      
      mount_point_replacement_attachments.each{
        |mount_point, attachment|
        unless attachment.exists?()
          next
        end
        
        begin
          TU.cmd_result("#{TI.sudo_prefix()}/bin/mount #{mount_point}")
        rescue CommandError => ce
          TU.error("There was an error while mounting #{mount_point}")
        end
      }
    rescue CommandError => ce
      TU.exception(ce)
    rescue => e
      raise e
    ensure
      # These snapshots should always be deleted since they are not tracked
      # anywhere else
      temporary_snapshots.each{
        |snapshot|
        begin
          if snapshot.exists?()
            TU.debug("Delete snapshot #{snapshot.id} that was created just for the restore")
            snapshot.delete()
          end
        rescue => e
          TU.debug(e)
        end
      }
      
      if TU.is_valid?() != true && rollback_volumes == true
        TU.debug("Detach and remove the new volumes due to an error")
        remove_volume_attachments(mount_point_replacement_attachments)
        mount_point_replacement_volumes.each{
          |mount_point,volume|
          if volume.exists?()
            if [:available, :error].include?(volume.status)
              begin
                volume.delete()
              rescue
                # Eat these errors since there is nothing else
                # we can do at this point
              end
            end
          end
        }
      end
    end
  end
  
  def get_ec2
    if opt(:access_key_id) != nil
      AWS.config({
        :access_key_id => opt(:access_key_id)
      })
      ENV["AWS_ACCESS_KEY"] = opt(:access_key_id)
    end
    if opt(:secret_access_key) != nil
      AWS.config({
        :secret_access_key => opt(:secret_access_key)
      })
      ENV["AWS_SECRET_KEY"] = opt(:secret_access_key)
    end
    ec2 = AWS::EC2.new()
    
    ec2
  end
  
  def get_instance
    ec2 = get_ec2()
    
    instance_id = TU.cmd_result("/opt/aws/bin/ec2-metadata -i | cut -d':' -f2 | tr -d ' '")
    if instance_id.to_s() == ""
      raise "Unable to determine the instance ID for this host"
    end
    
    region = get_region()
    instance = region.instances[instance_id]
    unless instance.exists?()
      raise "Unable to find instance #{instance_id} in #{region.name}"
    end
    
    instance
  end
  
  def get_region
    ec2 = get_ec2()
    
    region_id = TU.cmd_result("/opt/aws/bin/ec2-metadata -z | grep -Po [a-z]+-[a-z]+-[0-9]+")
    if region_id.to_s() == ""
      raise "Unable to find the instance region for this host"
    end
    
    ec2.regions[region_id]
  end
  
  def get_mount_point(path)
    TU.cmd_result("df -P #{path} | tail -1 | cut -d' ' -f 1")
  end
  
  def find_attachment_for_mount_point(mount_point, instance)
    search = [mount_point]
    matches = mount_point.match("/dev/xvd([a-z]+)")
    if matches && matches.size() > 0
      search << "/dev/sd#{matches[1]}"
    end
    
    attachment = nil
    instance.block_device_mappings().each{
      |device_name,block_device|
      if search.include?(device_name)
        attachment = block_device
      end
    }
    
    if attachment == nil
      raise "Unable to find an EBS volume for #{mount_point} on instance #{instance.id}"
    end
    
    attachment
  end
  
  def wait_for_snapshots(snapshots)
    snapshots.each{
      |snapshot|
      sleep 15 until [:completed, :error].include?(snapshot.status)

      case snapshot.status
      when :completed
        TU.debug("The copy to snapshot #{snapshot.id} has finished")
      when :error
        TU.error("The copy to snapshot #{snapshot.id} failed")
      end
    }
  end
  
  def remove_volume_attachments(attachments, delete_volumes = true)
    attachments.each{
      |mount_point, attachment|
      if attachment.exists?()
        TU.debug("Detach volume at #{mount_point}")
        attachment.delete()
      end
    }

    TU.debug("Waiting for all volumes to be detached")
    attachments.each{
      |mount_point, attachment|
      sleep 15 until [:available, :error].include?(attachment.volume.status)

      case attachment.volume.status
      when :available
        TU.debug("The detachment of volume #{attachment.volume.id} has finished")
      when :error
        TU.error("The detachment of volume #{attachment.volume.id} failed")
      end
    }

    if delete_volumes == true
      attachments.each{
        |mount_point, attachment|
        if attachment.exists?() == false
          begin
            TU.debug("Delete volume #{attachment.volume.id} for #{mount_point}")
            attachment.volume.delete()
          rescue => e
            TU.exception(e)
          end
        else
          TU.error("The #{mount_point} volume did not fully detach")
        end
      }
    end
  end
  
  def delete_unretained_snapshots(retained_backups)
    retained_snapshot_ids = {}
    retained_backups.each{
      |backup|
      backup.each{
        |path,info|
        if info["snapshot_id"] == nil
          # Skip because this path does not have a snapshot_id
          # This can happen if multiple paths are located on the same volume
          next
        end
        
        retained_snapshot_ids[info["snapshot_id"]] = info
      }
    }
    
    instance = get_instance()
    region = get_region()
    region.snapshots.with_tag("ContinuentTungstenSnapshotInstanceID", instance.id).with_tag("ContinuentTungstenBackupMethod", self.class.name).each {
      |snapshot|
      unless retained_snapshot_ids.has_key?(snapshot.id)
        TU.debug("Remove unretained snapshot #{snapshot.id}")
        snapshot.delete()
      end
    }
  end
  
  def validate
    super()
  end
  
  def configure
    super()
    
    add_option(:access_key_id, {
      :on => "--access-key-id String"
    })
    
    add_option(:secret_access_key, {
      :on => "--secret-access-key String"
    })
    
    add_option(:delete_original_volumes, {
      :on => "--delete-original-volumes String",
      :parse => method(:parse_boolean_option),
      :default => true,
      :help => "Delete the original volume following a successful restore"
    })
  end
  
  def require_stopped_dataserver_for_backup?
    if opt(:lock_tables) == true
      false
    else
      true
    end
  end
  
  def script_name
    "ebs_snapshot.sh"
  end
end