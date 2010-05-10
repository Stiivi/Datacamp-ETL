# ETL Job
#
# Copyright:: (C) 2009 Knowerce, s.r.o.
# 
# Author:: Stefan Urbanek
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

require 'etl/etl_job_status'

class Job
attr_accessor :argument
attr_reader   :job_status

attr_reader :connection
attr_reader :files_directory
attr_reader :name
attr_reader :config
attr_reader :defaults_domain
attr_accessor :job_status
attr_accessor :defaults
attr_accessor :last_run_date

def initialize(manager, bundle)
	@manager = manager
	@name = bundle.name
	@bundle = bundle
	@log = manager.log
end

def prepare
    # do nothing
end

def defaults_domain=(domain)
    @defaults_domain = domain
    @config = @manager.domains_config[@defaults_domain.to_s]
end

def status=(status)
    @job_status.status = status
	@job_status.save
end

def status
	return @job_status.status
end

def message=(message)
	@job_status.message = message
	@job_status.save
end

def message
	return @job_status.message
end

def phase= (phase)
	@job_status.phase = phase
	@job_status.save
end

def phase
	return @job_status.phase
end

def launch_with_argument(argument, options = {})
	if options[:debug]
		debug = true
	end
	
	@argument = argument

	# FIXME: Prepare defaults
	# job.defaults_domain = job_info.name if job.defaults_domain.nil?
	
	# job.defaults = ETLDefaults.new(job.defaults_domain)
	# job.last_run_date = job_info.last_run_date

	@job_status = ETLJobStatus.new

	start_time = Time.now
	@job_status.job_name = @name
	@job_status.start_time = start_time
	@job_status.status = "init"	
	@job_status.save
	
	prepare

	# FIXME: prefix log as job log
	@job_status.status = "running"
	@job_status.save
	
    if not debug
        begin
    		run
        rescue
            self.status = "failed"
            self.message = $!.message
        end
    else
        run
    end    

	if self.status != "failed"
		@job_status.status = "ok"
		@job_status.message = nil
	end
	
	end_time = Time.now
    @job_status.end_time = end_time
	@job_status.save

	finalize

    job_elapsed_time = ((end_time - start_time) * 100).round / 100

	if @job_status.status == "failed"
		@log.error "Job '#{@name}' failed: #{@job_status.message}"
	end

    @log.info "Job '#{name}' finished. Status: #{self.status}. Elapsed time: #{job_elapsed_time}s"
end

def run
	raise NotImplementedError, "ETL Job subclasses should implement the 'run' method."
end

def fail(message)
    @job_status.status = "failed"
    @job_status.message = message
    @job_status.end_time = Time.now
    @job_status.save
end

def finalize
	# Override in sublcasses
end

def log
    @manager.log
end

def files_directory
    @manager.files_directory_for_job(self)
end

def staging_system_columns
    @manager.staging_system_columns
end

def execute_sql(sql_statement)
    # FIXME: store SQL statement in DB table
	raise RuntimeError, "execute_sql not implemented yet"
end

end
