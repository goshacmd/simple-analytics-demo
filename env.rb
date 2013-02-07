require 'bundler/setup'
require 'mongoid'

Mongoid.configure.connect_to 'analytics_demo'

require './models'
