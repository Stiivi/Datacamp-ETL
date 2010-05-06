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

class Job
attr_accessor :argument
attr_accessor :status
attr_accessor :message
attr_accessor :phase
attr_accessor :start_time
attr_accessor :end_time
attr_accessor :status_id

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
	@connection = manager.connection
end

def prepare
    # do nothing
end

def defaults_domain=(domain)
    @defaults_domain = domain
    @config = @manager.domains_config[@defaults_domain.to_s]
end

def status=(status)
    @status = status
	# FIXME: save job status
    @manager.update_job_status(self)
end

def message=(message)
    @message = message
	# FIXME: save job status
    @manager.update_job_status(self)
end

def phase= (phase)
#	@log "phase #{phase}"
    @phase = phase
	# FIXME: save job status
    @manager.update_job_status(self)
end

def run
# Do nothing by default
# FIXME: shoud raise exception that this has to be overriden
end

def fail(message)
    @status = "failed"
    @message = message
    @end_time = Time.now
	# FIXME: save job status
    @manager.update_job_status(self)
end

def finalize
    @status = "ok" if @status == "running"
    @end_time = Time.now
    @manager.update_job_status(self)
	# FIXME: save job status
end

def log
    @manager.logger
end

def files_directory
    @manager.files_directory_for_job(self)
end

def staging_system_columns
    @manager.staging_system_columns
end

def execute_sql(sql_statement)
    # FIXME: store SQL statement in DB table
    # FIXME: uncomment this /production
    @connection << sql_statement
end

end
