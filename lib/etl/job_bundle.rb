# ETL Job Info
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

class JobBundle
attr_reader :name
attr_reader :defaults_domain
attr_reader :type
attr_reader :job_class_name

attr_reader :is_loaded
cattr_accessor :job_search_path

@@named_bundles = nil
@@job_search_path = []

def self.path_for_job(name)
    @@job_search_path.each { |search_path|
        job_path = Pathname.new("#{search_path}/#{name}.etl")
        return job_path if job_path.directory?        
    }
    return nil
end

def self.bundle_with_name(name)
	if !@@named_bundles
		@@named_bundles = Hash.new
	end
	
	bundle = @@named_bundles[name]
	if bundle
		return bundle
	end
	
	path = path_for_job(name)
	if not path
		return nil
	end

	# Reuse bundles if already loaded
	bundle = self.new(path)
	@@named_bundles[name] = bundle
	return bundle
end


def initialize(path)
	@is_loaded = false

	# Just in case, so we are sure that we have Pathname object
	@path = Pathname.new(path)
	
	basename = @path.basename
	@name = basename.to_s.gsub(/\.[^.]*$/, "")
	
	info_file = @path + 'info.yml'

	if info_file.exist?
		@info = YAML.load_file(info_file)
	else
		@info = {}
	end
	
	@type = @info[:job_type]
	if not @type
		if basename =~ /\.rb$/
			@type = "ruby"
		elsif (@path + "#{name}.rb").exist?
			@type = "ruby"
		end
	end
	
	@job_class_name = @info["job_class"]
	if not @job_class_name
		@job_class_name = @name.camelize + "ETLJob"
	end
end

def load
	if @is_loaded
		return
	end
	
	if @type == "ruby"
		load_ruby_job
	else
		raise RuntimeError, "Unknown job type '#{@type}' for job '#{@name}'"
	end
end

def load_ruby_job
	###############################
	# Get executable file
	ruby_executable = @info["executable"]
	
	if not ruby_executable
		ruby_executable = "#{@name}.rb"
	end
	
	ruby_file = @path + ruby_executable
	
    if not ruby_file.exist?
	    raise RuntimeError, "Unable to find ruby file #{ruby_file}"
	    return nil
    end
	
	# Perform actual load
	require ruby_file
	
	@is_loaded = true
end

def job_class
	if not @is_loaded
		load
	end

	# FIXME: this is for ruby only

	if not Class.class_exists?(@job_class_name)
        raise RuntimeError, "Undefined class #{@job_class_name} for job '#{name}'"
	end

    job_class = Class.class_with_name(@job_class_name)

	@@job_superclass = Job

	if not job_class.is_kind_of_class(@@job_superclass)
        raise RuntimeError, "Class #{job_class} is not kind of of #{@@job_superclass}"
    end
    
    return job_class
end

end
