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

require 'lib/job_status'
require 'lib/etl_defaults'

class Job
attr_reader :connection
attr_reader :table_prefix, :schema,  :files_directory
attr_reader :name
attr_reader :config
attr_accessor :job_status
attr_accessor :defaults
attr_reader :defaults_domain
attr_accessor :last_run_date
attr_accessor :info

def initialize(manager)
    @manager = manager
    @connection = @manager.connection
    @schema = @manager.staging_schema
    @table_prefix = "sta_"
    @config = @manager.domains_config[@defaults_domain.to_s]
    if not @config
        @config = Hash.new
    end
end

def name
    return @info.name
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
end

def message=(message)
    @job_status.message = message
end
def phase=(phase)
    @job_status.phase = phase
    @job_status.save
end

def status
    @job_status.status
end

def message
    @job_status.message
end

def phase
    @job_status.phase
end

def fail(message)
    @job_status.status = "failed"
    @job_status.message = message
end

def run
end

def finalize
    @job_status.status = "ok" if @job_status.status == "running"
    @job_status.end_date = Time.now

    @job_status.save
end

def logger
    # FIXME: depreciated
    @manager.logger
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
