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
	@manager = manager
	@domain = domain
end

def default_association(key)
	return ETLDefaultAssociation.first( {:domain => @domain, :default_key => key} )
end

def [](key)
	default = default_association(key)
	if not default
		return nil
	end
	return default.default_value
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
	default = default_association(key)

	if default
		default.value = value.to_s
		default.save
	else
		default = ETLDefaultAssociation.new
		default.domain = @domain
		default.default_key = key.to_s
		default.default_value = value
		default.save
	end
end

def delete(key)
	default = default_association(key)
	if default
		default.destroy
	end
end

end
