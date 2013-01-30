require 'td'
require 'td-client'
require 'mysql'
require './settings.rb'

load_settings('tdrsettings.yaml')

cln = TreasureData::Client.new(@tdapikey)
cln.databases.each { |db|
  db.tables.each { |tbl|
    p tbl.db_name
    p tbl.table_name
    p tbl.count
  }
}