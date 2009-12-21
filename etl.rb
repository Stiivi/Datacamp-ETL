#! /usr/bin/ruby
#
# Script for running ETL
#
# For more info use: ruby etl.rb --help
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

require 'lib/job_manager'
require 'optparse'

class ETLTool

def initialize
    @options = {}

    OptionParser.new do |opts|
        opts.banner = "Usage: etl.rb [options] [jobs]"
        
        opts.on("-c", "--config CONFIG", "Specify configuration file.") do |config|
            @options[:config] = config
        end
        opts.on("-d", "--debug", "Enable debugging") do
            @debug = true
        end
        opts.separator "\n"
        opts.separator "* [jobs] is optional list of jobs to be run in form: name.type. Example: data.extraction"
        opts.separator "* if no jobs are specified, then scheduled jobs are run"
        opts.separator "* default configuration file is config.yml in current directory"
    end.parse!

    @jobs = ARGV

    if @options[:config].nil?
        config_file = Pathname.new("config.yml")
    else
        config_file = Pathname.new(@options[:config])
    end

    if not config_file.exist?
        raise "Configuration file '#{config_file}' does not exist"
    end
    @configuration = YAML.load_file(config_file)
    
    if @configuration["log_file"]
        @log_file = @configuration["log_file"]
    else
        @log_file = STDERR
    end
end

def create_job_manager
    @job_manager = JobManager.new
    if @debug
        @job_manager.debug = true
    end

    begin
        @job_manager.establish_connection(@configuration["connection"])
        @job_manager.log_file = @log_file
        @job_manager.staging_schema = @configuration["staging_schema"]
        @job_manager.dataset_schema = @configuration["dataset_schema"]
        
        if @configuration["job_search_path"]
            @job_manager.job_search_path = @configuration["job_search_path"]
        else
            @job_manager.job_search_path = [Pathname.new(__FILE__).dirname + "jobs"]
        end
        
        if @configuration["etl_files_path"]
            @job_manager.etl_files_path = @configuration["etl_files_path"]
        end
        
        @job_manager.configuration = @configuration
    rescue => exception
        @job_manager.log.error "#{exception.message}"
        @job_manager.log.error exception.backtrace.join("\n")
    end
end
   
def run_scheduled_jobs
    begin
        @job_manager.run_scheduled_jobs
    rescue => exception
        @job_manager.log.error "#{exception.message}"
        @job_manager.log.error exception.backtrace.join("\n")
    end
end

def run_jobs(jobs)
    jobs.each { |job|
        split = job.split(".")
        job_name = split[0]
        if split.count == 1
            job_type = "job"
        else
            job_type = split[1]
        end
        @job_manager.run_job_with_name(job_name, job_type)
    }
end

def run
    create_job_manager
    @job_manager.log.info "ETL start"
    if not @jobs.empty?
        run_jobs(@jobs)
    else
        run_scheduled_jobs
    end
    @job_manager.log.info "ETL finished"
end

end

tool = ETLTool.new
tool.run
