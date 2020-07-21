require "thor"

module S3inventory
  class Cli < Thor
    class_option :profile

    desc "ls-bucket", "List buckets"
    def ls_bucket
      buckets = s3.list_buckets.buckets.map(&:name)
      buckets.sort.each {|b| puts b }
    end

    desc "ls-inventory BUCKET", "List inventories for bucket"
    def ls_inventory(bucket)
      Inventory.new(s3, bucket).configurations.each {|c| puts c.id }
    end

    desc "ls-manifest BUCKET INVENTORY", "List manifests for inventory"
    def ls_manifest(bucket, inventory)
      Inventory.new(s3, bucket).manifests(inventory).each {|m| puts m.key }
    end

    desc "cat BUCKET INVENTORY MANIFEST_KEY", "Print concatenated csv"
    def cat(bucket, inventory, manifest_key)
      Inventory.new(s3, bucket).cat(inventory, manifest_key)
    end

    private
    def s3
      @s3 ||= Aws::S3::Client.new(profile: options[:profile], region: ENV["AWS_REGION"] || "ap-northeast-1")
    end
  end
end
