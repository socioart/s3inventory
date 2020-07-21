require "s3inventory/version"
require "aws-sdk-s3"

module S3inventory
  class Error < StandardError; end
  # Your code goes here...
end

require "s3inventory/cli"
require "s3inventory/inventory"
