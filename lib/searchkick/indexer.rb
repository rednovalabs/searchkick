module Searchkick
  class Indexer
    attr_reader :queued_items

    def initialize
      @queued_items = []
    end

    def queue(items)
      @queued_items.concat(items)
      perform unless Searchkick.callbacks_value == :bulk
    end

    def perform
      items = @queued_items
      @queued_items = []
      if items.any?
        response = Searchkick.client.bulk(body: items)
        if response["errors"]
          item_responses = response["items"].map do |item|
            (item["index"] || item["delete"] || item["update"])
          end
          failures, successes = item_responses.partition { |item| item["error"] }
          first_with_error = failures.first
          error = Searchkick::ImportError.new "#{first_with_error["error"]} on item with id '#{first_with_error["_id"]}'"
          error.failures = failures
          raise error
        end
      end
    end
  end
end
