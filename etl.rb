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

require 'etl'
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

def create_etl_manager
    @etl_manager = ETLManager.new
    if @debug
        @etl_manager.debug = true
    end

    begin
        @etl_manager.domains_config = @configuration["domains"]
        @etl_manager.establish_connection(@configuration["connection"])
        @etl_manager.log_file = @log_file

        @etl_manager.staging_schema = @configuration["staging_schema"]
        @etl_manager.dataset_schema = @configuration["dataset_schema"]
        
        if @configuration["job_search_path"]
            JobBundle.job_search_path = @configuration["job_search_path"]
        else
            JobBundle.job_search_path = [Pathname.new(__FILE__).dirname + "jobs"]
        end
        
        if @configuration["etl_files_path"]
            @etl_manager.etl_files_path = @configuration["etl_files_path"]
        end
        
        @etl_manager.configuration = @configuration
    rescue => exception
        @etl_manager.log.error "#{exception.message}"
        @etl_manager.log.error exception.backtrace.join("\n")
    end
end
   
def run_scheduled_jobs
    begin
        @etl_manager.run_scheduled_jobs
    rescue => exception
        @etl_manager.log.error "#{exception.message}"
        @etl_manager.log.error exception.backtrace.join("\n")
    end
end

def run_jobs(jobs)
    jobs.each { |job|
        @etl_manager.run_named_job(job)
    }
end

def run
    create_etl_manager
    @etl_manager.log.info "ETL start"
    if not @jobs.empty?
        run_jobs(@jobs)
    else
        run_scheduled_jobs
    end
    @etl_manager.log.info "ETL finished"
end

end

tool = ETLTool.new
tool.run
