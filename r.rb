require 'elasticsearch'
require 'json'
require 'pp'

client = Elasticsearch::Client.new log: true
client.transport.reload_connections!

AWS_INDEX = 'aws'

Dir.foreach("#{Dir.pwd}/") do |fname|
  next unless fname.end_with?(".json")

  content = File.read fname
  doc     = JSON.parse content

  doc['configurationItems'].each do |ci|
    body = Hash.new
    events = []
    begin
      existed_doc = client.get index: AWS_INDEX, type: 'resource', id: ci["resourceId"]
      body = existed_doc["_source"]
      events = existed_doc["_source"]["events"].reject { |e| e.nil? }
    rescue Exception => e
    end

    if body["tags"].nil?
      body["tags"] = Hash.new
    end

    body["tags"].merge!(ci["tags"])

    body.merge!({
        "awsAccountId" => ci["awsAccountId"],
        "resourceType" => ci["resourceType"],
        "resourceId"   => ci["resourceId"],

        "resourceCreationTime"=> body["resourceCreationTime"] || ci["resourceCreationTime"],
        "awsRegion"=> body["awsRegion"] || ci["awsRegion"],
        "availabilityZone"=> body["availabilityZone"] || ci["availabilityZone"],
        "ARN"=> body["ARN"] || ci["ARN"],
        "events" => events,
    })

    begin
      if ci["configuration"]
        body.merge!({
        "launchTime"=> ci["configuration"]["launchTime"],
        "vpcId"=> ci["configuration"]["vpcId"],
        "instanceId"=> ci["configuration"]["instanceId"],
        })

        body['events'] << {
        "configurationItemStatus" => ci["configurationItemStatus"],
        "configurationItemCaptureTime" => ci["configurationItemCaptureTime"],
        "clientToken" => ci["configuration"]["clientToken"],
        "keyName" => ci["configuration"]["keyName"],
        "stateReason"=> ci["configuration"]["stateReason"],
        }
      end
    rescue Exception => e
      pp e
    end

    puts "\n\nInserted body"
    pp body
    puts ">>>>>>>>>>>>>>>>"

    client.index index: AWS_INDEX, type: 'resource', id: ci['resourceId'], body: body
  end
end
