spec = Gem::Specification.new do |s| 
  s.name = "datacamp-etl"
  s.version = "0.2.0"
  s.author = "Stefan Urbanek"
  s.email = "stefan.urbanek@gmail.com"
  s.homepage = "http://github.com/Stiivi/Datacamp-ETL/"
  s.platform = Gem::Platform::RUBY
  s.summary = "Datacamp Extraction-Transformation-Loading library"
  s.files = Dir['lib/**/*.rb'] + Dir['bin/*'] + Dir['test/**/*']
  s.require_path = "lib"
  s.has_rdoc = true
  s.extra_rdoc_files = ["README"]
  s.executables << 'etl'
end
