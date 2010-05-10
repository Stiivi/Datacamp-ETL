class ETLDefaultAssociation
	include DataMapper::Resource

	property :id,            Serial
	property :domain,        String
	property :default_key,   String, :field => "key"
	property :default_value, String, :field => "value"
end
