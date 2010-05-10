class ConnectionManager
attr_reader :connection_search_path

@@default_manager = nil


def self.default_manager
	if !@@default_manager
		@@default_manager = self.new
	end
	return @@default_manager
end

def initialize
	@named_connections = Hash.new
	@connection_search_path = Array.new
end

def connection_search_path= (path)
	@connection_search_path = path
	reload_connection_info
end

def reload_connection_info
	@named_connection_infos = Hash.new
	
    @connection_search_path.each { |search_path|
    	path = Pathname.new(search_path)
		path.children.each { |file|
			hash = YAML.load_file(file)
			@named_connection_infos.merge!(hash)
		}
    }
end

def available_connections
	return @named_connection_infos.keys
end

def named_connection_info(name)
	return @named_connection_infos[name.to_s]
end

def create_connection(info_name, identifier = nil)
	# FIXME: rename to create_named_connection
	info = named_connection_info(info_name)
	if info
		connection = Sequel.connect(info)
		
		if identifier
			add_named_connection(connection, identifier)
		end
	else
		connection = nil
	end
	return connection
end

def add_named_connection(connection, identifier)
	@named_connections[identifier] = connection
end

def remove_named_connection(identifier)
	@named_connection.delete(identifier)
end

def named_connection(name)
	return @named_connections[name]
end

end
