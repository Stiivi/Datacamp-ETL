# TaskManager - manage ETL tasks
#
# Copyright (C) 2009 Knowerce, s.r.o.
# 
# Written by: Stefan Urbanek
# Date: Oct 2009
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'rubygems'
require 'sequel'
require 'yaml'
require 'pathname'
require 'activerecord'
require 'lib/task'
require 'lib/task_info'
require 'lib/task_status'
require 'logger'

DEBUG = true

class TaskManager

attr_reader :connection
attr_reader :staging_schema, :dataset_schema
attr_writer :dataset_schema
attr_reader :files_path
attr_reader :etl_files_path, :tasks_path
attr_reader :configuration
attr_writer :configuration
attr_reader :logger
attr_writer :logger

@@staging_system_columns = [:id, :date_created, :etl_loaded_date]

def initialize
	@defaults = StagingDefaults.new("etl")
	@etl_files_path = Pathname.new("files")
    @tasks_path = Pathname.new("tasks")

	return self
end

def establish_connection(connection_info)
    # Create database connection
    
    @connection_info = connection_info
    @connection = Sequel.mysql(@staging_schema,
            :user => connection_info["username"],
            :password => connection_info["password"], 
            :host => connection_info["host"],
            :encoding => 'utf8'
            )

    Sequel::MySQL.default_charset = 'utf8'

	if @connection.nil?
		raise "Unable to establish database connection"
	end
end
    
def staging_schema=(schema)
	@staging_schema = schema
	ActiveRecord::Base.establish_connection(
	  :adapter => "mysql",
	  :host => @connection_info["host"],
	  :username => @connection_info["username"],
	  :password => @connection_info["password"],
	  :database => @staging_schema,
	  :encoding => 'utf8')
end

def load_task_class(task_name, task_type)
	# FIXME: define root directory
    
    module_dir = Pathname.new("#{@tasks_path}/#{task_type.pluralize}/#{task_name}")
    if not module_dir.exist?
      @logger.error "Unable to find task (#{task_type}) directory '#{module_dir}'"
      return nil
    end

    base_name = "#{task_name.downcase}_#{task_type}.rb"

    file = module_dir + base_name

    if not file.exist?
	    @logger.error "Unable to find #{task_type} class file #{file}"
	    return nil
    end

    require file
    
    class_name = task_name.camelize + task_type.capitalize
    task_class = Kernel.const_get(class_name)

	return task_class
end

def run_enabled_tasks_of_type(task_type)
	run_tasks(TaskInfo.find_enabled(task_type))
end

def run_scheduled_tasks_of_type(task_type)
	force = @defaults.bool_value("force_run_all")

	if force
        @logger.info "Forcing all scheduled #{task_type} tasks"
		tasks = TaskInfo.find_enabled(task_type)
		# FIXME: reset this flag for @production
		# @defaults["force_run_all"] = "false"
	else
		tasks = TaskInfo.find_scheduled(task_type)
	end

    if tasks.nil? or tasks.empty?
        @logger.info "No #{task_type} tasks to run"
    end

	run_tasks(tasks)
	
    # resed force run flag (make the task to be run according to schedule)
	tasks.each { |task|
	    task.force_run = 0
	}
end

def run_tasks(task_infos)
	task_infos.each {|info|
		@logger.info "Running #{info.task_type} task #{info.name} (#{info.id})"
		run_task_with_info(info)
	}
end

def run_task_with_info(task_info)
	error = false

	begin
		task_class = load_task_class(task_info.name, task_info.task_type)
		if task_class.nil?
		    raise "Unable to load task class for #{task_info.name} (#{task_info.task_type}), id = #{task_info.id}"
        end
		task = task_class.new(self)
    rescue => exception
        @logger.error "Task #{task_info.name}(#{task_info.task_type}) failed: #{$!.message}"
        @logger.error exception.backtrace.join("\n")

		fail_task(task_info, $!.message)
		return
	end

	# Prepare task status
	task.info = task_info
	task.task_status = TaskStatus.new
	task.task_status.status = "running"
	task.task_status.start_date = Time.now
	task.task_status.task_name = task_info.name
	task.task_status.task_id = task_info.id
	task.task_status.save
	
	# Prepare defaults
	task.defaults_domain = task_info.name if task.defaults_domain.nil?
	
	task.defaults = StagingDefaults.new(task.defaults_domain)
	task.last_run_date = task_info.last_run_date

	task.prepare

    if not DEBUG
        begin
            task.run
            task.finalize
        rescue
            task.status = "failed"
            task.message = $!.message
            task.finalize
        end
    else
        task.run
        task.finalize
    end    
	task_info.last_run_status = task.task_status.status
	task_info.last_run_date = task.task_status.end_date
	task_info.save
	if task.status == "failed"
		@logger.error "Task #{task_info.name}(#{task_info.task_type}) failed: #{task.message}"
	end
end

def fail_task(task_info, message)
	now = Time.now
	status = TaskStatus.new
	status.status = "failed"
	status.start_date = now
	status.end_date = now
	status.task_name = task_info.name
	status.task_id = task_info.id
	status.message = message
	status.save
	task_info.last_run_status = "failed"
	task_info.last_run_date = now
	task_info.save
end
def staging_system_columns
	return @@staging_system_columns
end
def etl_files_path=(path)
    @etl_files_path = Pathname.new(path)
end
def tasks_path=(path)
    @tasks_path = Pathname.new(path)
end
def files_directory_for_task(task)
    path = @etl_files_path + task.name.underscore

    if path.exist?
        if path.directory?
            return path
        else
            raise "Path #{path} is a file, not a directory"
        end
    end
    
    path.mkpath()
    
    return path
end
end
