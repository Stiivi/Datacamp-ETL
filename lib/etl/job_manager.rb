# JobManager - manage ETL jobs
#
# Copyright:: (C) 2009 Knowerce, s.r.o.
# 
# Author:: Stefan Urbanek
# Date:: Oct 2009
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

require 'yaml'
require 'pathname'
require 'etl/job'
require 'etl/job_info'
require 'etl/job_status'
require 'etl/download_manager'

# Main ETL class that manages all jobs. Use this object to:
# * prepare and queue jobs
# * run jobs
# * schedule jobs
# * configure database connection

class JobManager

attr_reader :connection
attr_accessor :staging_schema, :dataset_schema
attr_accessor :configuration
attr_accessor :log
attr_accessor :debug
attr_accessor :domains_config
attr_reader :files_path
attr_reader :etl_files_path, :jobs_path
attr_accessor :job_search_path

@@staging_system_columns = [:id, :date_created, :etl_loaded_date]
@@job_superclass = Job

# Create new instance of ETL job manager. Defaults domain for the ETL
# manager is set to _etl_, default path for file storage is ./files and
# jobs are searched in ./jobs
def initialize
	@defaults = ETLDefaults.new("etl")
	@etl_files_path = Pathname.new("files")
    @jobs_path = Pathname.new("jobs")
    log_file = STDERR
	return self
end

def debug=(debug_flag)
    # prevent some other values
    if debug_flag
        @debug = true
    else
        @debug = false
    end
    if @log
        if @debug
            @log.level = Logger::DEBUG
        else
            @log.level = Logger::INFO
        end
     end
end

def run_job_with_info(job_info)
	error = false

    @log.info "running job #{job_info.name}.#{job_info.job_type}"

    job_start_time = Time.now

	begin
		job_class = load_job_class(job_info.name, job_info.job_type)
		if job_class.nil?
		    raise "Unable to load job class for #{job_info.name} (#{job_info.job_type}), id = #{job_info.id}"
        end
		job = job_class.new(self)
    rescue => exception
        @log.error "Job #{job_info.name}(#{job_info.job_type}) failed: #{$!.message}"
        @log.error exception.backtrace.join("\n")

		fail_job(job_info, $!.message)
		return
	end

	# Prepare job status
	job.info = job_info
	job.job_status = JobStatus.new
	job.job_status.status = "running"
	job.job_status.start_date = Time.now
	job.job_status.job_name = job_info.name
	job.job_status.job_id = job_info.id
	job.job_status.save
	
	# Prepare defaults
	job.defaults_domain = job_info.name if job.defaults_domain.nil?
	
	job.defaults = ETLDefaults.new(job.defaults_domain)
	job.last_run_date = job_info.last_run_date

	job.prepare

    if not @debug
        begin
            job.run
            job.finalize
        rescue
            job.status = "failed"
            job.message = $!.message
            job.finalize
        end
    else
        job.run
        job.finalize
    end    
	job_info.last_run_status = job.job_status.status
	job_info.last_run_date = job.job_status.end_date
	job_info.save
	if job.status == "failed"
		@log.error "Job #{job_info.name}(#{job_info.job_type}) failed: #{job.message}"
	end

    job_elapsed_time = ((Time.now - job_start_time) * 100).round / 100

    @log.info "job #{job_info.name}.#{job_info.job_type} finished. time: #{job_elapsed_time} s status:#{job.status}"
end

def fail_job(job_info, message)
	now = Time.now
	status = JobStatus.new
	status.status = "failed"
	status.start_date = now
	status.end_date = now
	status.job_name = job_info.name
	status.job_id = job_info.id
	status.message = message
	status.save
	job_info.last_run_status = "failed"
	job_info.last_run_date = now
	job_info.save
end
def staging_system_columns
	return @@staging_system_columns
end
def etl_files_path=(path)
    @etl_files_path = Pathname.new(path)
end
def jobs_path=(path)
    @jobs_path = Pathname.new(path)
end
def files_directory_for_job(job)
    path = @etl_files_path + job.name.underscore

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
def logger
    # FIXME: put depreciation warning here
    return @log
end
end
