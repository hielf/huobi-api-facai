require 'yaml'

def config_from_file(config_path)
  configs = if File.exist?(config_path)
              YAML.load_file(config_path, symbolize_names: true)
            end
  configs || {}
end