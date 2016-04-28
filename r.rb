require 'elasticsearch'
require 'json'
require 'pp'

client = Elasticsearch::Client.new log: true
client.transport.reload_connections!


Dir.foreach("#{Dir.pwd}/") do |fname|
  next unless fname.end_with?(".json")

  content = File.read fname
  doc     = JSON.parse content

  doc['configurationItems'].each do |ci|
    events = []
    begin
      existed_doc = client.get index: 'aws', type: 'resource', id: ci["resourceId"]
      events = existed_doc["_source"]["events"].reject { |e| e.nil? }
    rescue Exception => e
    end

    begin
      body = {
        "tags"=> ci["tags"],
        "awsAccountId"=> ci["awsAccountId"],
        "resourceType" => ci["resourceType"],
        "resourceId"   => ci["resourceId"],

        "resourceCreationTime"=> ci["resourceCreationTime"],
        "awsRegion"=> ci["awsRegion"],
        "availabilityZone"=> ci["availabilityZone"],
        "ARN"=> ci["ARN"],
        "events" => events
      }

      if ci["configuration"]
        body['events'] << {
        "configurationItemStatus" => ci["configurationItemStatus"],
        "configurationItemCaptureTime" => ci["configurationItemCaptureTime"],
        "clientToken" => ci["configuration"]["clientToken"],
        "keyName" => ci["configuration"]["keyName"],
        "launchTime"=> ci["configuration"]["launchTime"],
        "vpcId"=> ci["configuration"]["vpcId"],
        "instanceId"=> ci["configuration"]["instanceId"],
        "stateReason"=> ci["configuration"]["stateReason"],
        }
      end
    rescue Exception => e
      pp e
    end

    puts "\n\nInserted body"
    pp body
    puts ">>>>>>>>>>>>>>>>"

    client.index index: 'aws', type: 'resource', id: ci['resourceId'], body: body
  end
end
