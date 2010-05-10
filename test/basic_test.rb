require 'test/unit'
require 'rubygems'
require 'etl'

class BasicTest < Test::Unit::TestCase
def setup
	@manager = ETLManager.new('sqlite3::memory:')
	@manager.create_etl_manager_structures
end

def test_defaults
	defaults = @manager.defaults_for_domain("etl")
	assert_not_nil(defaults)
	
	value = defaults[:count]
	assert_equal(nil, value)
	
	value = defaults.value(:count, 10).to_i
	assert_equal(10, value)

	defaults[:flag] = "yes"
	value = defaults.bool_value(:flag)
	assert_equal(true, value)

	# should keep 10
	value = defaults.value(:count, 20).to_i
	assert_equal(10, value)

	defaults.delete(:count)
	# should reset to new default 20
	value = defaults.value(:count, 20).to_i
	assert_equal(20, value)
end
end
