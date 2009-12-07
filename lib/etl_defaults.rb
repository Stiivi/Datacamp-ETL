# Configuration for ETL tasks
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

require 'lib/staging_default_association'

class StagingDefaults

def initialize(domain)
	@domain = domain
end

def [](key)
	default = StagingDefaultAssociation.find(:first, :conditions => ["domain = ? and default_key = ?", @domain, key.to_s])
	if default.nil?
		return nil
	else
		return default.value
	end
end

def value(key, default_value)
	default = StagingDefaultAssociation.find(:first, :conditions => ["domain = ? and default_key = ?", @domain, key.to_s])
	if default.nil?
		self[key] = default_value.to_s
		return self[key]
	else
		return default.value
	end
end

def bool_value(key)
	default = StagingDefaultAssociation.find(:first, :conditions => ["domain = ? and default_key = ?", @domain, key.to_s])
	if default.nil? or default.value.nil?
		return false
	else
		value = default.value
		if value == 1 or value.downcase == "true" or value.downcase == "yes"
			return true
		else
			return false
		end
	end
end


def []=(key, value)
	default = StagingDefaultAssociation.find(:first, :conditions => ["domain = ? and default_key = ?", @domain, key.to_s])
	if default.nil?
		default = StagingDefaultAssociation.new
		default.domain = @domain
		default.default_key = key.to_s
	end
	default.value = value.to_s
	default.save
end

def delete(key)
	default = StagingDefaultAssociation.find(:first, :conditions => ["domain = ? and default_key = ?", @domain, key.to_s])
	if not default.nil?
		StagingDefaultAssociation.delete(default.id)
	end
end

end
