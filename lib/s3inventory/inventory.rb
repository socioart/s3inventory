module S3inventory
  class Inventory
    # include Loggable
    # extend Forwardable

    # delegate %i(s3 inventory_name name) => :@bucket
    attr_reader :s3, :bucket

    def initialize(s3, bucket)
      @s3 = s3
      @bucket = bucket
    end

    def configurations
      configurations = enumerate {|token|
        s3.list_bucket_inventory_configurations(bucket: bucket, continuation_token: token)
      }.map(&:inventory_configuration_list).flatten(1)

      configurations.sort_by(&:id)
    end

    def manifests(inventory)
      config = inventory_configuration(bucket, inventory).inventory_configuration
      manifests = enumerate {|token|
        s3.list_objects_v2(
          bucket: config.destination.s3_bucket_destination.bucket.gsub(/^arn:aws:s3:::/, ""),
          prefix: [config.destination.s3_bucket_destination.prefix, bucket, inventory, "2"].compact.join("/"), # 2 is prefix of timestamp
          continuation_token: token,
        )
      }.map {|resp|
        resp.contents.filter {|obj|
          obj.key.end_with?("manifest.json")
        }
      }.flatten(1)

      manifests.sort_by(&:key)
    end

    def cat(inventory, manifest_key)
      out = $stdout
      config = inventory_configuration(bucket, inventory).inventory_configuration
      destination_bucket = config.destination.s3_bucket_destination.bucket.gsub(/^arn:aws:s3:::/, "")
      manifest = JSON.parse(
        s3.get_object(
          bucket: destination_bucket,
          key: manifest_key,
        ).body.read,
      )

      out.puts manifest["fileSchema"].split(/,\s+/).join(",") # header
      manifest["files"].each do |file|
        warn "Retrieve inventry data for bucket #{bucket} (key: #{file["key"]})"
        body = s3.get_object(bucket: destination_bucket, key: file["key"]).body # sio
        warn "Retrieved inventry data for bucket #{bucket} (key: #{file["key"]})"

        tempfile = Tempfile.new
        IO.copy_stream(body, tempfile)
        tempfile.rewind

        out.write(Zlib::GzipReader.new(tempfile).read)
      end
    end

    private
    def enumerate(&block)
      Enumerator.new do |y|
        token = nil
        loop do
          resp = block.call(token)
          y << resp
          token = resp.continuation_token
          break unless token
        end
      end
    end

    def inventory_configuration(bucket, inventory)
      s3.get_bucket_inventory_configuration(bucket: bucket, id: inventory)
    end
  end
end
