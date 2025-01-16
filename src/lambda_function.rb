require 'json'
require 'aws-sdk-dynamodb'
require 'aws-sdk-sns'
require 'net/http'

def lambda_handler(event:, context:)
  # Initialize AWS SDK
  dynamodb = Aws::DynamoDB::Client.new
  sns = Aws::SNS::Client.new

  begin
    # fetch weather data
    weather_data = fetch_weather_data
    puts "Weather data: #{weather_data}"

    # save to DynamoDB
    save_to_database(dynamodb, weather_data)
    puts "Data saved to database"

    # check alert conditions
    if check_alert_conditions(weather_data)
      send_alert(sns, weather_data)
      puts "Alert sent"
    end

    # clean up old data during each execution
    cleanup_old_data(dynamodb)
    puts "Old data cleanup completed"
    
    {
      statusCode: 200,
      body: JSON.generate({ message: 'Success' })
    }
  rescue StandardError => e
    puts "Error: #{e.message}"
    puts e.backtrace
    raise e
  end
end

def fetch_weather_data
  api_key = ENV['OPENWEATHERMAP_API_KEY']
  city_id = '1850147' #id of tokyo
  url = "https://api.openweathermap.org/data/2.5/weather?id=#{city_id}&appid=#{api_key}&units=metric"

  uri = URI(url)
  response = Net::HTTP.get_response(uri)
  data = JSON.parse(response.body)

  {
    'location_id' => 'tokyo',
    'timestamp' => Time.now.to_i,
    'temperature' => data['main']['temp'],
    'humidity' => data['main']['humidity'],
    'rainfall' => data.dig('rain', '1h').to_f,  # default = 0 if no data
    'wind_speed' => data['wind']['speed']
  }
end

def save_to_database(dynamodb, data)
  dynamodb.put_item({
    table_name: 'weather_records',
    item: data
  })
end

def check_alert_conditions(data)
  # basic alert conditions
  data['temperature'] > 30 || data['rainfall'] > 30
end

def send_alert(sns, data)
  puts "Alert condition met: #{data}"
end

def cleanup_old_data(dynamodb)
  # delete data older than one month
  one_month_ago = (Time.now - 30*24*60*60).to_i
  
  dynamodb.scan({
    table_name: 'weather_records',
    filter_expression: '#ts < :old_date',
    expression_attribute_names: {
      '#ts' => 'timestamp'
    },
    expression_attribute_values: {
      ':old_date' => one_month_ago
    }
  }).items.each do |item|
    dynamodb.delete_item({
      table_name: 'weather_records',
      key: {
        'location_id' => item['location_id'],
        'timestamp' => item['timestamp']
      }
    })
  end
end
