# mappings = YAML.load(File.open('mapping.yml'))

# (['events', 'properties', 'event_properties'] - DB.tables).each do |table|
#   DB[table].columns
# end

module Helpers

  def mappings
    mappings = YAML.load(File.open('mapping.yml')).with_indifferent_access
  end

  def client
    client = Mixpanel::Client.new(
      api_key:    '8f0e0cce36c6b4cf2e6ca32b172d3087',
      api_secret: '23f86e1d979ebef0742edcf89eef5bdc'
    )
  end

  def logger
    l = Logger.new(STDOUT)
    l.level = Logger::DEBUG
    l
  end

  def update_tables(db)
    (mappings.keys.map(&:to_sym) - db.tables).each do |table|
      db.create_table table do
        primary_key :id
        binding.pry if mappings[table].nil?
        mappings[table].each do |key, column|
          send column['type'], key
        end
      end
    end
  end

  def last_fetch(db)
    begin
      db[:events].order(:id).last[:time].to_date.strftime
    rescue Exception => exception
      logger.warn(exception.message)
      logger.info('looks like it is the first time you import it. It will take sometime')
      # '2011-07-10'
      '2016-06-15'
    end
  end


  def prepare_attributes(datum, attribute)
    prepared = {}
    mappings[attribute].map do |column, map|
      prepared.merge! column => typcasting(datum['properties'][map[:mixpanel]], map[:type].constantize)
    end
    prepared
  end

  def typcasting(string, type)
    if type == Integer
      string.to_i
    elsif type == DateTime
      Time.at(string.to_i).to_datetime
    else
      string
    end
  end
end