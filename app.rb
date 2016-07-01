require 'rubygems'
require 'bundler/setup'
require 'irb'
require 'yaml'
require 'logger'
require './helpers'

Bundler.require(:default)

include Helpers

logger.info('connecting to database')
DB = Sequel.connect("postgres://#{ENV['POSTGRES_USERNAME']}:#{ENV['POSTGRES_PASSWORD']}@localhost:5432/mixpanel_imports")

logger.info('updating database information')
update_tables(DB)

logger.info('Fetching data from mixpanel')
data = client.request(
  'export',
  from_date: last_fetch(DB),
  to_date: (Time.now.utc - 7.hour).to_date.strftime,
)

logger.info(data.length.to_s + ' of events fetched')
latest_events = DB[:events].order(:time).last.try(:[], :time).to_i
count = 0
data.each do |datum|
  # skip exisiting records
  next if datum['properties']['time'] <= DB[:events].order(:time).last.try(:[], :time).to_i
  puts typcasting(datum['properties']['time'], 'DateTime').to_i
  puts typcasting(latest_events, 'DateTime').to_i
  puts '==='
  user_id = DB[:users].insert(prepare_attributes(datum, :users))
  DB[:events].insert(prepare_attributes(datum, :events).merge('user_id' => user_id, 'name' => datum[:event]))
  count += 1
end

logger.info(count.to_s + ' of data is imported')

logger.info('All done.')
