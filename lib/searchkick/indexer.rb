module Searchkick
  class Indexer
    attr_reader :queued_items

    def initialize
      reset_queue!
    end

    def queue(items)
      @queued_items.concat(items)
      perform unless Searchkick.callbacks_value == :bulk
    end

    def perform
      items = @queued_items
      reset_queue!
      return if items.none?

      disable_shipping_es2 = Flipper.new(Flipper::Adapters::ActiveRecord.new).enabled?(:flipper_fms_disable_shipping_es2)
      enable_shipping_es7 = Flipper.new(Flipper::Adapters::ActiveRecord.new).enabled?(:flipper_fms_enable_shipping_es7)

      response = Searchkick.client.bulk(body: items) if !disable_shipping_es2

      Er::Client.new.bulk_index(items) if enable_shipping_es7

      raise_bulk_indexing_exception!(response) if (response['errors'] && !disable_shipping_es2)
    end

    private

    def reset_queue!
      @queued_items = []
    end

    def raise_bulk_indexing_exception!(response)
      item_responses = response["items"].map do |item|
        (item["index"] || item["delete"] || item["update"])
      end

      failures, successes = item_responses.partition { |item| item["error"] }
      first_with_error = failures.first

      e = Searchkick::ImportError.new "#{first_with_error["error"]} on item with id '#{first_with_error["_id"]}'. Succeeded: #{successes.size}, Failed: #{failures.size}"
      e.failures = failures

      raise e
    end
  end
end
