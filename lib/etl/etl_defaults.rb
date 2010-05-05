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

require 'etl/etl_default_association'

class ETLDefaults

def initialize(manager, domain)
	@connection = manager.connection
	@defaults = @connection[:etl_defaults]
	@domain = domain
end

def default_record(key)
	sel = @defaults.filter(["domain = ? and default_key = ?", 
						@domain, key.to_s])
	if sel.count > 0
		return sel.all[0]
	else
		return nil
	end
end

def [](key)
	default = default_record(key)
	if not default
		return nil
	end
	return default[:value]
end

def value(key, default_value)
	value = self[key]

	if not value
		self[key] = default_value.to_s
		return self[key]
	else
		return value
	end
end

def bool_value(key)
	value = self[key]
	if not value
		return false
	else
		if value == 1 or value.downcase == "true" or value.downcase == "yes"
			return true
		else
			return false
		end
	end
end



def []=(key, value)
	default = default_record(key)

	if default
		default[:value] = value.to_s
		@defaults.filter(:id=>default[:id]).update(default)
	else
		default = { :domain => @domain,
					:default_key => key.to_s,
					:value => value}
		@defaults.insert(default)
	end
end

def delete(key)
	default = default_record(key)
	if default
		@defaults.filter(:id=>default[:id]).delete
	end
end

end
